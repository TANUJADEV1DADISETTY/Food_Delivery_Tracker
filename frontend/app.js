const API_URL = 'http://localhost:5000/state';
const POLL_INTERVAL_MS = 2000;

const STAGES = ['PLACED', 'CONFIRMED', 'PREPARING', 'OUT_FOR_DELIVERY', 'DELIVERED'];

let activeOrdersState = {};
let totalCompletedCount = 0;

document.addEventListener('DOMContentLoaded', () => {
  fetchState();
  setInterval(fetchState, POLL_INTERVAL_MS);
});

async function fetchState() {
  const dot = document.getElementById('connectionDot');
  const text = document.getElementById('connectionText');

  try {
    const res = await fetch(API_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const data = await res.json();
    dot.className = 'pulse-dot online';
    text.textContent = 'API Connected';

    updateDashboard(data);
  } catch (err) {
    dot.className = 'pulse-dot offline';
    text.textContent = 'API Disconnected';
    console.warn('Error fetching state from API:', err);
  }
}

function updateDashboard(newState) {
  // Detect orders that disappeared (were delivered and removed from active state)
  const currentKeys = Object.keys(newState);
  const oldKeys = Object.keys(activeOrdersState);

  oldKeys.forEach(key => {
    if (!newState[key]) {
      totalCompletedCount++;
    }
  });

  activeOrdersState = newState;

  // Update summary stats
  const activeCount = currentKeys.length;
  document.getElementById('activeOrdersCount').textContent = activeCount;
  document.getElementById('totalProcessedCount').textContent = totalCompletedCount;

  let placed = 0;
  let preparing = 0;
  let delivery = 0;

  Object.values(newState).forEach(order => {
    const st = order.status;
    if (st === 'PLACED' || st === 'CONFIRMED') placed++;
    else if (st === 'PREPARING') preparing++;
    else if (st === 'OUT_FOR_DELIVERY') delivery++;
  });

  document.getElementById('placedCount').textContent = placed;
  document.getElementById('preparingCount').textContent = preparing;
  document.getElementById('deliveryCount').textContent = delivery;

  // Render cards
  const grid = document.getElementById('ordersGrid');
  const emptyState = document.getElementById('noOrdersState');

  if (activeCount === 0) {
    emptyState.style.display = 'flex';
    grid.innerHTML = '';
    return;
  }

  emptyState.style.display = 'none';

  // Render cards for active orders
  grid.innerHTML = Object.values(newState)
    .sort((a, b) => a.order_id.localeCompare(b.order_id))
    .map(order => createOrderCardHtml(order))
    .join('');
}

function createOrderCardHtml(order) {
  const currentIdx = STAGES.indexOf(order.status);
  const progressPercent = currentIdx >= 0 ? (currentIdx / (STAGES.length - 1)) * 100 : 0;

  const itemsHtml = (order.items || [])
    .map(item => `<span class="item-badge">${escapeHtml(item)}</span>`)
    .join('');

  const stepsHtml = STAGES.map((stage, idx) => {
    let cls = 'step';
    if (idx === currentIdx) cls += ' active';
    else if (idx < currentIdx) cls += ' completed';

    const stepNum = idx < currentIdx ? '✓' : idx + 1;
    const shortLabel = stage.replace('_', ' ');

    return `
      <div class="${cls}">
        <div class="step-circle">${stepNum}</div>
        <div class="step-label">${shortLabel}</div>
      </div>
    `;
  }).join('');

  return `
    <div class="order-card" id="card-${order.order_id}">
      <div class="order-header">
        <div>
          <div class="order-id-tag">${escapeHtml(order.order_id)}</div>
          <div class="restaurant-name">📍 ${escapeHtml(order.restaurant || 'Restaurant')}</div>
        </div>
        <span class="status-pill ${order.status}">${escapeHtml(order.status)}</span>
      </div>

      <div class="customer-info">
        <span class="customer-name">👤 ${escapeHtml(order.customer_name || 'Customer')}</span>
        <span class="est-delivery">⏱️ ~${order.estimated_delivery_minutes || 20} mins</span>
      </div>

      <div class="items-list">
        ${itemsHtml}
      </div>

      <div class="progress-tracker">
        <div class="progress-bar-fill" style="width: calc(${progressPercent}% - ${progressPercent * 0.4}px)"></div>
        ${stepsHtml}
      </div>
    </div>
  `;
}

function escapeHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}
