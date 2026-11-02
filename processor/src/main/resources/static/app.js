const RULE_NAMES = {
    VELOCITY: 'Ráfaga',
    IMPOSSIBLE_TRAVEL: 'Viaje imposible',
    AMOUNT_SPIKE: 'Pico de importe',
};
const MAX_ALERTS = 60;
const SVG_NS = 'http://www.w3.org/2000/svg';

const number = new Intl.NumberFormat('es-ES');
const money = new Intl.NumberFormat('es-ES', { style: 'currency', currency: 'EUR' });
const clock = new Intl.DateTimeFormat('es-ES', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
const hourMinute = new Intl.DateTimeFormat('es-ES', { hour: '2-digit', minute: '2-digit' });

const $ = id => document.getElementById(id);

function renderStats(stats) {
    const closed = stats.perMinute.slice(0, -1);
    $('kpi-rate').textContent = closed.length ? number.format(closed.at(-1).count) : '–';
    $('kpi-accepted').textContent = number.format(stats.accepted);
    $('kpi-alerts').textContent = number.format(stats.alerts);
    $('kpi-suppressed').textContent = number.format(stats.suppressedAlerts);
    $('kpi-invalid').textContent = number.format(stats.invalid);
    $('kpi-duplicates').textContent = number.format(stats.duplicates);
    renderChart(stats.perMinute);
    renderCountries(stats.byCountry);
}

function svg(name, attributes, text) {
    const element = document.createElementNS(SVG_NS, name);
    Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, value));
    if (text !== undefined) element.textContent = text;
    return element;
}

function renderChart(points) {
    const chart = $('chart');
    const [width, height, left, bottom] = [640, 220, 40, 22];
    const plotHeight = height - bottom - 8;
    const max = Math.max(10, ...points.map(point => point.count));
    const step = Math.ceil(max / 4 / 10) * 10 || 10;
    const scale = plotHeight / (step * 4);
    chart.replaceChildren();

    for (let i = 0; i <= 4; i++) {
        const y = height - bottom - i * step * scale;
        chart.append(svg('line', { class: 'grid-line', x1: left, x2: width, y1: y, y2: y }));
        chart.append(svg('text', { x: left - 6, y: y + 4, 'text-anchor': 'end' }, number.format(i * step)));
    }

    const slot = (width - left) / 16;
    points.forEach((point, index) => {
        const barHeight = point.count * scale;
        const x = left + index * slot + 3;
        const isOpen = index === points.length - 1;
        const bar = svg('rect', {
            class: isOpen ? 'bar open' : 'bar', x, y: height - bottom - barHeight,
            width: slot - 6, height: barHeight, rx: 3,
        });
        bar.append(svg('title', {}, `${hourMinute.format(new Date(point.minute))}: ${number.format(point.count)} operaciones, ${money.format(point.totalEur)}`));
        chart.append(bar);
        if (index % 3 === 0) {
            chart.append(svg('text', { x: x + (slot - 6) / 2, y: height - 6, 'text-anchor': 'middle' }, hourMinute.format(new Date(point.minute))));
        }
    });
}

function renderCountries(countries) {
    const max = Math.max(1, ...countries.map(country => country.count));
    $('countries').replaceChildren(...countries.map(country => {
        const row = document.createElement('li');
        const code = document.createElement('span');
        code.textContent = country.country;
        const track = document.createElement('div');
        track.className = 'track';
        const fill = document.createElement('div');
        fill.className = 'fill';
        fill.style.width = `${country.count / max * 100}%`;
        track.append(fill);
        const value = document.createElement('span');
        value.className = 'value';
        value.textContent = number.format(country.count);
        row.append(code, track, value);
        return row;
    }));
}

function alertItem(alert, fresh) {
    const item = document.createElement('li');
    item.className = `item rule-${alert.rule}${fresh ? ' fresh' : ''}`;
    item.dataset.id = alert.alertId;

    const rule = document.createElement('span');
    rule.className = 'rule';
    rule.textContent = RULE_NAMES[alert.rule] ?? alert.rule;

    const text = document.createElement('span');
    text.className = 'alert-text';
    const card = document.createElement('b');
    card.textContent = alert.cardId;
    text.append(card, alert.reason);
    text.title = alert.reason;

    const time = document.createElement('span');
    time.className = 'alert-time';
    time.textContent = clock.format(new Date(alert.occurredAt));

    item.append(rule, text, time);
    item.addEventListener('click', () => showCard(alert.cardId));
    return item;
}

function addAlert(alert, fresh = true) {
    const list = $('alerts');
    if (list.querySelector(`[data-id="${alert.alertId}"]`)) return;
    list.querySelector('.empty')?.remove();
    list.prepend(alertItem(alert, fresh));
    while (list.children.length > MAX_ALERTS) list.lastElementChild.remove();
}

async function showCard(cardId) {
    const panel = $('card');
    const response = await fetch(`/api/cards/${encodeURIComponent(cardId)}`);
    if (!response.ok) {
        panel.replaceChildren(Object.assign(document.createElement('p'), { className: 'empty', textContent: `Sin datos de ${cardId} todavía.` }));
        return;
    }
    const card = await response.json();
    const rows = [
        ['Operaciones en la última ventana', number.format(card.transactionsLastWindow)],
        ['Importe en esa ventana', money.format(card.amountLastWindowEur)],
        ['Historial', `${number.format(card.historySize)} operaciones`],
        ['Gasto habitual', money.format(card.usualAmountEur)],
        ['Último pago presencial', card.lastCity ? `${card.lastCity} (${card.lastCountry})` : '–'],
        ['Visto por última vez', card.lastSeenAt ? clock.format(new Date(card.lastSeenAt)) : '–'],
    ];
    const title = Object.assign(document.createElement('h3'), { textContent: card.cardId });
    const list = document.createElement('dl');
    rows.forEach(([label, value]) => {
        list.append(Object.assign(document.createElement('dt'), { textContent: label }),
            Object.assign(document.createElement('dd'), { textContent: value }));
    });
    panel.replaceChildren(title, list);
}

function setLive(on) {
    const badge = $('live');
    badge.className = `live ${on ? 'on' : 'off'}`;
    badge.textContent = on ? 'En vivo' : 'Reconectando…';
}

async function loadRecentAlerts() {
    const response = await fetch('/api/alerts?limit=30');
    if (!response.ok) return;
    const body = await response.json();
    (body._embedded?.fraudAlertList ?? []).reverse().forEach(alert => addAlert(alert, false));
}

const alertEvents = new EventSource('/api/alerts/events');
alertEvents.onopen = () => setLive(true);
alertEvents.onerror = () => setLive(false);
alertEvents.addEventListener('alert', event => addAlert(JSON.parse(event.data)));

const statsEvents = new EventSource('/api/stats/events');
statsEvents.addEventListener('stats', event => renderStats(JSON.parse(event.data)));

loadRecentAlerts();
