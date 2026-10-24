/* =========================================================
   rabbit.lab — simulator v2
   Учебная модель RabbitMQ: топология, routing, TTL, DLX,
   consumers, проверки и экспорт. Без реального брокера.
   ========================================================= */
(function () {
  "use strict";

  const STORAGE_KEY = "rl_sandbox_v2";
  const LEGACY_KEY = "rl_sandbox_v1";
  const MAX_MESSAGES = 200;
  const MAX_LOG = 120;
  const GRID = 4;

  const $ = (selector, root) => (root || document).querySelector(selector);
  const $$ = (selector, root) => Array.from((root || document).querySelectorAll(selector));
  const clone = (value) => JSON.parse(JSON.stringify(value));
  const clamp = (value, min, max) => Math.max(min, Math.min(max, value));
  const pad = (value) => String(value).padStart(2, "0");

  function esc(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function nowTime() {
    const date = new Date();
    return [pad(date.getHours()), pad(date.getMinutes()), pad(date.getSeconds())].join(":");
  }

  function shortNumber(value) {
    const number = Number(value) || 0;
    if (number >= 1000000) return (number / 1000000).toFixed(1).replace(".0", "") + "m";
    if (number >= 10000) return (number / 1000).toFixed(1).replace(".0", "") + "k";
    return String(number);
  }

  function slug(value) {
    return String(value || "node")
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, "-")
      .replace(/^-+|-+$/g, "") || "node";
  }

  function uid(prefix, seq) {
    return prefix + "_" + String(seq || Date.now().toString(36)) + Math.random().toString(36).slice(2, 5);
  }

  /* ---------- seeded topologies ---------- */

  function X(name, type, x, y, extra) {
    return Object.assign({
      id: name, name: name, type: type, x: x, y: y,
      durable: true, autoDelete: false, internal: false,
      alternateExchange: "", desc: ""
    }, extra || {});
  }

  function Q(name, x, y, extra) {
    return Object.assign({
      id: name, name: name, x: x, y: y,
      durable: true, autoDelete: false, ttl: 0, maxLength: 0,
      overflow: "drop-head", dlx: "", dlRoutingKey: "",
      maxPriority: 0, desc: ""
    }, extra || {});
  }

  function P(name, exchangeId, key, x, y) {
    return { id: "producer_" + slug(name), name: name, exchangeId: exchangeId, defaultKey: key, x: x, y: y, desc: "" };
  }

  function C(name, queueId, x, y, extra) {
    return Object.assign({
      id: "consumer_" + slug(name), name: name, queueId: queueId,
      x: x, y: y, active: true, autoAck: true, prefetch: 10, delivered: 0, desc: ""
    }, extra || {});
  }

  const PAYLOAD_ORDER = '{"order_id": 1842, "total": 7990, "currency": "RUB"}';

  const SCENARIOS = [
    {
      id: "direct", num: "01", label: "direct", mode: "guided",
      kicker: "Топология 01 · direct",
      title: "Direct: доставка по ключу",
      sub: "ключ → очередь",
      task: 'Опубликуйте сообщение с routing key <code>order.created</code>. Его должны получить <b>orders</b> и <b>audit</b>, но не <b>payments</b>.',
      desc: 'Опубликуйте сообщение с routing key <code>order.created</code>. Его должны получить <code>orders</code> и <code>audit</code>, но не <code>payments</code>.',
      hint: "Exchange типа direct сравнивает routing key сообщения с ключом binding. Проверьте биндинги очередей orders и audit.",
      success: "Direct работает: ключ совпал ровно с нужными bindings.",
      exchanges: [X("shop.direct", "direct", 330, 230, { kind: "main", desc: "Основной exchange магазина" })],
      queues: [
        Q("orders", 660, 70, { desc: "Заказы на обработку" }),
        Q("audit", 660, 225, { desc: "Журнал событий заказов" }),
        Q("payments", 660, 380, { desc: "Платёжный обработчик" })
      ],
      bindings: [
        { exchange: "shop.direct", queue: "orders", key: "order.created" },
        { exchange: "shop.direct", queue: "audit", key: "order.created" }
      ],
      producers: [P("order-app", "shop.direct", "order.created", 50, 250)],
      consumers: [],
      pub: { source: "producer_order-app", key: "order.created", payload: PAYLOAD_ORDER, headers: "type=order", count: 1 },
      checks: [
        { id: "pub.direct", label: "Опубликовано сообщение order.created" },
        { id: "queue.orders", label: "orders получила сообщение" },
        { id: "queue.audit", label: "audit получила сообщение" },
        { id: "queue.payments.none", label: "payments не получила сообщение" }
      ]
    },
    {
      id: "fanout", num: "02", label: "fanout", mode: "guided",
      kicker: "Топология 02 · fanout",
      title: "Fanout: вещание всем",
      sub: "игнорирует ключ",
      task: 'Опубликуйте сообщение в <code>shop.fanout</code>. Все три очереди — <b>email</b>, <b>inventory</b> и <b>analytics</b> — должны получить копию.',
      desc: 'Fanout игнорирует routing key и копирует сообщение в каждую связанную очередь. Добейтесь доставки во все три очереди.',
      hint: "У fanout нет ключа маршрутизации: важен сам факт binding. Проверьте, что все три очереди связаны с shop.fanout.",
      success: "Fanout разослал сообщение во все очереди.",
      exchanges: [X("shop.fanout", "fanout", 330, 240, { kind: "main", desc: "Вещательный exchange" })],
      queues: [
        Q("email", 660, 80, { desc: "Письма клиентам" }),
        Q("inventory", 660, 235, { desc: "Резервирование товара" }),
        Q("analytics", 660, 390, { desc: "События аналитики" })
      ],
      bindings: [
        { exchange: "shop.fanout", queue: "email", key: "" },
        { exchange: "shop.fanout", queue: "inventory", key: "" },
        { exchange: "shop.fanout", queue: "analytics", key: "" }
      ],
      producers: [P("shop-app", "shop.fanout", "order.created", 50, 260)],
      consumers: [],
      pub: { source: "producer_shop-app", key: "order.created", payload: PAYLOAD_ORDER, headers: "type=order", count: 1 },
      checks: [
        { id: "pub.fanout", label: "Опубликовано сообщение в fanout" },
        { id: "queue.email", label: "email получила сообщение" },
        { id: "queue.inventory", label: "inventory получила сообщение" },
        { id: "queue.analytics", label: "analytics получила сообщение" }
      ]
    },
    {
      id: "topic", num: "03", label: "topic", mode: "guided",
      kicker: "Топология 03 · topic",
      title: "Topic: маски и ключи",
      sub: "* и # в routing key",
      task: 'Опубликуйте <code>order.created.eu</code>. Ожидаются <b>eu.orders</b> и <b>all.orders</b>; <b>us.orders</b> и <b>payments</b> — нет.',
      desc: 'Topic сравнивает routing key с масками binding: <code>*</code> — ровно одно слово, <code>#</code> — ноль или больше слов.',
      hint: "Binding order.# подходит под order.created.eu. Binding order.created.us — нет. Маска payment.* не должна совпасть.",
      success: "Topic-маски сработали правильно: доставка только в eu.orders и all.orders.",
      exchanges: [X("shop.topic", "topic", 330, 245, { kind: "main", desc: "События с иерархическим ключом" })],
      queues: [
        Q("eu.orders", 675, 60, { desc: "Заказы региона EU" }),
        Q("us.orders", 675, 205, { desc: "Заказы региона US" }),
        Q("all.orders", 675, 350, { desc: "Все заказы для аудита" }),
        Q("payments", 675, 495, { desc: "Платёжные события" })
      ],
      bindings: [
        { exchange: "shop.topic", queue: "eu.orders", key: "order.created.eu" },
        { exchange: "shop.topic", queue: "us.orders", key: "order.created.us" },
        { exchange: "shop.topic", queue: "all.orders", key: "order.#" },
        { exchange: "shop.topic", queue: "payments", key: "payment.*" }
      ],
      producers: [P("region-app", "shop.topic", "order.created.eu", 50, 265)],
      consumers: [],
      pub: { source: "producer_region-app", key: "order.created.eu", payload: PAYLOAD_ORDER, headers: "region=eu", count: 1 },
      checks: [
        { id: "pub.topic", label: "Опубликовано order.created.eu" },
        { id: "queue.eu.orders", label: "eu.orders получила сообщение" },
        { id: "queue.all.orders", label: "all.orders получила сообщение" },
        { id: "queue.us.orders.none", label: "us.orders не получила сообщение" },
        { id: "queue.payments.none", label: "payments не получила сообщение" }
      ]
    },
    {
      id: "dlq", num: "04", label: "dlq", mode: "guided",
      kicker: "Топология 04 · dead letter",
      title: "DLQ: мёртвые письма",
      sub: "nack → DLX → DLQ",
      task: 'Опубликуйте <code>order.created</code>, затем сделайте <b>nack</b> сообщения в <b>orders</b>. Оно должно уйти в <b>orders.dlq</b>, а <b>orders</b> — опустеть.',
      desc: 'Очередь может указать dead-letter exchange. При nack или reject сообщение уходит в DLX и дальше в DLQ — не теряется.',
      hint: "Откройте очередь orders в инспекторе и нажмите nack. Проверьте, что x-dead-letter-exchange = orders.dlx, а orders.dlq связана с orders.dlx.",
      success: "Dead-letter routing сработал: сообщение покинуло orders и оказалось в orders.dlq.",
      exchanges: [
        X("shop.direct", "direct", 300, 165, { kind: "main", desc: "Основной exchange" }),
        X("orders.dlx", "fanout", 300, 445, { kind: "dlx", desc: "Dead-letter exchange для orders" })
      ],
      queues: [
        Q("orders", 650, 125, { desc: "Рабочая очередь заказов", dlx: "orders.dlx", dlRoutingKey: "" }),
        Q("orders.dlq", 960, 435, { isDlq: true, desc: "Очередь мёртвых писем" })
      ],
      bindings: [
        { exchange: "shop.direct", queue: "orders", key: "order.created" },
        { exchange: "orders.dlx", queue: "orders.dlq", key: "" }
      ],
      producers: [P("order-app", "shop.direct", "order.created", 45, 185)],
      consumers: [],
      pub: { source: "producer_order-app", key: "order.created", payload: PAYLOAD_ORDER, headers: "type=order", count: 1 },
      checks: [
        { id: "pub.dlq", label: "Опубликовано order.created" },
        { id: "queue.orders.received", label: "orders получила сообщение" },
        { id: "nack.orders", label: "Сделан nack сообщения в orders" },
        { id: "queue.orders.empty", label: "orders пуста после nack" },
        { id: "queue.orders.dlq", label: "orders.dlq получила сообщение" }
      ]
    },
    {
      id: "retry", num: "05", label: "retry", mode: "guided",
      kicker: "Топология 05 · retry / TTL",
      title: "Retry: отложенная повторная доставка",
      sub: "TTL + DLX",
      task: 'Сделайте <b>nack</b> в <b>work</b>. Сообщение должно попасть в <b>retry</b>, подождать TTL и вернуться в <b>work</b> уже как redelivered.',
      desc: 'Retry-очередь держит сообщение заданное время (x-message-ttl), затем через DLX возвращает его в рабочую очередь. Это не busy-loop, а управляемая задержка.',
      hint: "В retry уже настроены TTL 2.5 с, DLX work.direct и routing key order.created. После nack дождитесь тика симуляции — сообщение вернётся в work.",
      success: "Retry-цикл замкнут: сообщение отложено, возвращено и помечено redelivered.",
      exchanges: [
        X("work.direct", "direct", 300, 165, { kind: "main", desc: "Рабочий exchange" }),
        X("retry.direct", "direct", 300, 450, { kind: "dlx", desc: "Exchange для отложенных сообщений" })
      ],
      queues: [
        Q("work", 650, 120, { desc: "Основная обработка", dlx: "retry.direct", dlRoutingKey: "order.retry" }),
        Q("retry", 650, 405, { desc: "Отложенная повторная доставка", ttl: 2500, dlx: "work.direct", dlRoutingKey: "order.created" })
      ],
      bindings: [
        { exchange: "work.direct", queue: "work", key: "order.created" },
        { exchange: "retry.direct", queue: "retry", key: "order.retry" }
      ],
      producers: [P("order-app", "work.direct", "order.created", 45, 185)],
      consumers: [],
      pub: { source: "producer_order-app", key: "order.created", payload: PAYLOAD_ORDER, headers: "type=order", count: 1 },
      checks: [
        { id: "pub.retry", label: "Опубликовано order.created" },
        { id: "nack.work", label: "Сделан nack в work" },
        { id: "retry.queued", label: "retry приняла сообщение" },
        { id: "retry.config", label: "У retry есть TTL и DLX на work.direct" },
        { id: "retry.returned", label: "work получила сообщение после TTL" }
      ]
    },
    {
      id: "headers", num: "06", label: "headers", mode: "guided",
      kicker: "Топология 06 · headers",
      title: "Headers: маршрутизация по атрибутам",
      sub: "x-match all / any",
      task: 'Опубликуйте сообщение с headers <code>type=order</code> и <code>region=eu</code>. Ожидаются <b>eu.orders</b> и <b>audit</b>; <b>priority.orders</b> — нет.',
      desc: 'Headers exchange игнорирует routing key и сравнивает headers сообщения с аргументами binding. <code>x-match=all</code> требует все пары, <code>any</code> — хотя бы одну.',
      hint: "В поле headers укажите две строки: type=order и region=eu. priority.orders ищет priority=high или region=us через x-match any — ни одна пара не совпадает.",
      success: "Headers-маршрутизация сработала по атрибутам, а не по ключу.",
      exchanges: [X("headers.route", "headers", 320, 255, { kind: "main", desc: "Маршрутизация по headers" })],
      queues: [
        Q("eu.orders", 680, 95, { desc: "Заказы EU" }),
        Q("audit", 680, 255, { desc: "Аудит заказов" }),
        Q("priority.orders", 680, 415, { desc: "Приоритетные заказы" })
      ],
      bindings: [
        { exchange: "headers.route", queue: "eu.orders", key: "", args: { "x-match": "all", type: "order", region: "eu" } },
        { exchange: "headers.route", queue: "audit", key: "", args: { "x-match": "all", type: "order" } },
        { exchange: "headers.route", queue: "priority.orders", key: "", args: { "x-match": "any", priority: "high", region: "us" } }
      ],
      producers: [P("headers-app", "headers.route", "", 45, 275)],
      consumers: [],
      pub: { source: "producer_headers-app", key: "", payload: PAYLOAD_ORDER, headers: "type=order\nregion=eu", count: 1 },
      checks: [
        { id: "pub.headers", label: "Опубликовано с type=order и region=eu" },
        { id: "queue.eu.orders", label: "eu.orders получила сообщение" },
        { id: "queue.audit", label: "audit получила сообщение" },
        { id: "queue.priority.orders.none", label: "priority.orders не получила сообщение" }
      ]
    },
    {
      id: "free", num: "07", label: "free", mode: "free",
      kicker: "Свободная схема · sandbox",
      title: "Свободная схема",
      sub: "полный конструктор",
      task: 'Свободный режим: добавьте узлы, соберите маршрутизацию и проверьте схему аудитом. Ограничений на топологию нет.',
      desc: 'Свободный режим: добавьте узлы, соберите маршрутизацию и проверьте схему аудитом. Ограничений на топологию нет.',
      hint: "Начните с exchange, затем добавьте очередь и binding. Producer подключается к exchange, consumer — к очереди.",
      success: "",
      exchanges: [], queues: [], bindings: [], producers: [], consumers: [],
      pub: { source: "", key: "order.created", payload: PAYLOAD_ORDER, headers: "type=order", count: 1 },
      checks: []
    }
  ];

  const GUIDED_SCENARIOS = SCENARIOS.filter((scenario) => scenario.checks.length > 0);

  function getScenario(id) {
    return SCENARIOS.find((scenario) => scenario.id === id) || SCENARIOS[0];
  }

  /* ---------- state ---------- */

  const states = Object.create(null);
  const completed = new Set();
  const histories = new Map();
  let currentId = "direct";
  let autosendTimer = null;
  let saveTimer = null;
  let clockSeconds = 59 * 60 + 59;
  let booted = false;

  function createState(scenario) {
    const seq = 10;
    const exchanges = clone(scenario.exchanges).map((exchange, index) => Object.assign({
      durable: true, autoDelete: false, internal: false, alternateExchange: "", desc: ""
    }, exchange, { x: Number(exchange.x) || 320, y: Number(exchange.y) || (140 + index * 150) }));
    const queues = clone(scenario.queues).map((queue, index) => Object.assign({
      durable: true, autoDelete: false, ttl: 0, maxLength: 0, overflow: "drop-head",
      dlx: "", dlRoutingKey: "", maxPriority: 0, desc: ""
    }, queue, {
      x: Number(queue.x) || 650, y: Number(queue.y) || (80 + index * 155),
      messages: [], received: 0, seen: {}, nacks: 0, dropped: 0, expired: 0, _rr: 0
    }));
    const bindings = clone(scenario.bindings || []).map((binding, index) => ({
      id: "b_seed_" + scenario.id + "_" + index,
      exchange: binding.exchange,
      queue: binding.queue,
      key: binding.key || "",
      args: binding.args || {}
    }));
    const producers = clone(scenario.producers || []).map((producer) => Object.assign({
      published: 0, desc: ""
    }, producer));
    const consumers = clone(scenario.consumers || []).map((consumer) => Object.assign({
      active: true, autoAck: true, prefetch: 10, delivered: 0, acked: 0, desc: ""
    }, consumer));
    const pub = Object.assign({ source: "", key: "", payload: PAYLOAD_ORDER, headers: "", count: 1 }, scenario.pub || {});
    if (!pub.source && producers[0]) pub.source = producers[0].id;
    return {
      id: scenario.id,
      exchanges: exchanges,
      queues: queues,
      bindings: bindings,
      producers: producers,
      consumers: consumers,
      publishes: [],
      metrics: { published: 0, routed: 0, acked: 0, deadLettered: 0, dropped: 0, returned: 0, expired: 0 },
      nacks: {},
      seq: seq,
      pub: pub,
      log: [
        { time: nowTime(), kind: "sys", text: "sandbox attached · vhost /s_8f21" },
        { time: nowTime(), kind: "sys", text: "rabbit.lab routing engine · confirms: on" },
        { time: nowTime(), kind: "sys", text: "scenario " + scenario.num + " loaded · " + exchanges.length + " exchange(s) · " + queues.length + " queue(s)" }
      ],
      taskState: "",
      view: null,
      speed: 1,
      paused: false
    };
  }

  function normalizeQueue(queue) {
    const next = Object.assign({
      durable: true, autoDelete: false, ttl: 0, maxLength: 0, overflow: "drop-head",
      dlx: "", dlRoutingKey: "", maxPriority: 0, desc: "", isDlq: false
    }, queue);
    next.id = queue.id || queue.name;
    next.name = queue.name || queue.id || "queue";
    next.x = Number(queue.x) || 650;
    next.y = Number(queue.y) || 100;
    next.ttl = Math.max(0, Number(queue.ttl) || 0);
    next.maxLength = Math.max(0, Number(queue.maxLength) || 0);
    next.messages = Array.isArray(queue.messages) ? queue.messages.filter((message) => message && message.id).slice(-MAX_MESSAGES) : [];
    next.messages.forEach((message) => {
      message.status = message.status === "unacked" ? "unacked" : "ready";
    });
    next.received = Number(queue.received) || 0;
    next.seen = queue.seen && typeof queue.seen === "object" ? queue.seen : {};
    next.nacks = Number(queue.nacks) || 0;
    next.dropped = Number(queue.dropped) || 0;
    next.expired = Number(queue.expired) || 0;
    next._rr = 0;
    return next;
  }

  function normalizeState(scenario, saved) {
    const base = createState(scenario);
    if (!saved || !Array.isArray(saved.exchanges) || !Array.isArray(saved.queues)) return base;
    const next = Object.assign({}, base, saved, { id: scenario.id });
    next.exchanges = saved.exchanges.map((exchange) => Object.assign({}, exchange, {
      id: exchange.id || exchange.name,
      name: exchange.name || exchange.id || "exchange",
      type: ["direct", "fanout", "topic", "headers"].indexOf(exchange.type) >= 0 ? exchange.type : "direct",
      x: Number(exchange.x) || 300,
      y: Number(exchange.y) || 150
    }));
    next.queues = saved.queues.map(normalizeQueue);
    next.bindings = (Array.isArray(saved.bindings) ? saved.bindings : []).map((binding, index) => ({
      id: binding.id || "b_" + index,
      exchange: binding.exchange,
      queue: binding.queue,
      key: binding.key || "",
      args: binding.args && typeof binding.args === "object" ? binding.args : {}
    })).filter((binding) => next.exchanges.some((exchange) => exchange.id === binding.exchange) && next.queues.some((queue) => queue.id === binding.queue));
    next.producers = (Array.isArray(saved.producers) ? saved.producers : []).map((producer, index) => Object.assign({
      id: "producer_" + index, name: "producer-" + (index + 1), exchangeId: "", defaultKey: "", x: 50, y: 200, published: 0, desc: ""
    }, producer));
    next.consumers = (Array.isArray(saved.consumers) ? saved.consumers : []).map((consumer, index) => Object.assign({
      id: "consumer_" + index, name: "consumer-" + (index + 1), queueId: "", x: 950, y: 200,
      active: true, autoAck: true, prefetch: 10, delivered: 0, acked: 0, desc: ""
    }, consumer));
    next.publishes = Array.isArray(saved.publishes) ? saved.publishes.slice(0, 40) : [];
    next.metrics = Object.assign({ published: 0, routed: 0, acked: 0, deadLettered: 0, dropped: 0, returned: 0, expired: 0 }, saved.metrics || {});
    next.nacks = saved.nacks && typeof saved.nacks === "object" ? saved.nacks : {};
    next.seq = Number(saved.seq) || 10;
    next.pub = Object.assign({}, base.pub, saved.pub || {});
    next.log = Array.isArray(saved.log) ? saved.log.slice(0, MAX_LOG) : base.log;
    next.view = saved.view && typeof saved.view === "object" ? saved.view : null;
    next.speed = [0.5, 1, 2, 4].indexOf(Number(saved.speed)) >= 0 ? Number(saved.speed) : 1;
    next.paused = !!saved.paused;
    return next;
  }

  function restoreSaved() {
    let saved = null;
    try {
      saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || "null");
    } catch (error) {
      saved = null;
    }
    if (saved && saved.version === 2 && saved.states) {
      SCENARIOS.forEach((scenario) => {
        const raw = saved.states[scenario.id];
        states[scenario.id] = raw ? normalizeState(scenario, raw) : createState(scenario);
      });
      (Array.isArray(saved.completed) ? saved.completed : []).forEach((id) => {
        if (SCENARIOS.some((scenario) => scenario.id === id && scenario.checks.length)) completed.add(id);
      });
      if (SCENARIOS.some((scenario) => scenario.id === saved.current)) currentId = saved.current;
    } else {
      SCENARIOS.forEach((scenario) => { states[scenario.id] = createState(scenario); });
      migrateLegacy();
    }
    const hash = window.location.hash.replace(/^#/, "");
    if (SCENARIOS.some((scenario) => scenario.id === hash)) currentId = hash;
  }

  function migrateLegacy() {
    let saved = null;
    try {
      saved = JSON.parse(localStorage.getItem(LEGACY_KEY) || "null");
    } catch (error) {
      saved = null;
    }
    if (!saved || saved.version !== 1 || !saved.states) return;
    (Array.isArray(saved.completed) ? saved.completed : []).forEach((id) => {
      if (SCENARIOS.some((scenario) => scenario.id === id && scenario.checks.length)) completed.add(id);
    });
    ["direct", "fanout", "topic", "dlq"].forEach((id) => {
      const old = saved.states[id];
      const scenario = getScenario(id);
      if (!old || !Array.isArray(old.queues)) return;
      const fresh = createState(scenario);
      fresh.bindings = [];
      old.queues.forEach((queue) => {
        const target = fresh.queues.find((item) => item.name === queue.name);
        if (!target) return;
        target.messages = Array.isArray(queue.messages) ? queue.messages : [];
        target.received = Number(queue.received) || 0;
        target.seen = queue.seen || {};
        (Array.isArray(queue.bindings) ? queue.bindings : []).forEach((binding) => {
          const exchange = fresh.exchanges.find((item) => item.name === binding.exchange);
          if (exchange) fresh.bindings.push({ id: uid("b", fresh.seq++), exchange: exchange.id, queue: target.id, key: binding.key || "", args: binding.args || {} });
        });
        if (queue.consumer) {
          fresh.consumers.push(C("consumer-" + queue.name, target.id, target.x + 280, target.y, { autoAck: true }));
        }
      });
      if (old.pub) fresh.pub = Object.assign(fresh.pub, old.pub);
      if (old.publishes) fresh.publishes = old.publishes;
      if (old.metrics) fresh.metrics = Object.assign(fresh.metrics, old.metrics);
      states[id] = fresh;
    });
  }

  function persistNow(showToast) {
    clearTimeout(saveTimer);
    saveTimer = null;
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({
        version: 2,
        current: currentId,
        completed: Array.from(completed),
        states: states
      }));
      if (showToast) toast("Топология и прогресс сохранены в этом браузере.", "Сохранено");
    } catch (error) {
      if (showToast) toast("Браузер не дал сохранить данные. Проверьте настройки приватности.", "Не сохранилось");
    }
  }

  function persistSoon() {
    clearTimeout(saveTimer);
    saveTimer = setTimeout(() => persistNow(false), 350);
  }

  /* ---------- lookups ---------- */

  function state() {
    return states[currentId] || (states[currentId] = createState(getScenario(currentId)));
  }

  const exchangeById = (target, id) => (target.exchanges || []).find((exchange) => exchange.id === id);
  const queueById = (target, id) => (target.queues || []).find((queue) => queue.id === id);
  const producerById = (target, id) => (target.producers || []).find((producer) => producer.id === id);
  const consumerById = (target, id) => (target.consumers || []).find((consumer) => consumer.id === id);
  const bindingsForExchange = (target, id) => (target.bindings || []).filter((binding) => binding.exchange === id);
  const bindingsForQueue = (target, id) => (target.bindings || []).filter((binding) => binding.queue === id);
  const consumersForQueue = (target, id) => (target.consumers || []).filter((consumer) => consumer.queueId === id);

  function mainExchange(target) {
    const list = target.exchanges || [];
    return list.find((exchange) => exchange.kind === "main") || list[0] || null;
  }

  function producerSource(target) {
    const id = target.pub && target.pub.source;
    return producerById(target, id) || (target.producers || [])[0] || null;
  }

  function uniqueName(target, kind, base) {
    const list = kind === "queue" ? target.queues : kind === "exchange" ? target.exchanges : kind === "producer" ? target.producers : target.consumers;
    let index = 1;
    let name = base;
    const used = new Set((list || []).map((item) => item.name));
    while (used.has(name)) {
      index += 1;
      name = base + "." + index;
    }
    return name;
  }

  function readyMessages(queue) {
    return (queue.messages || []).filter((message) => message.status !== "unacked");
  }

  function unackedMessages(queue) {
    return (queue.messages || []).filter((message) => message.status === "unacked");
  }

  function queueCounts(queue) {
    const ready = readyMessages(queue).length;
    const unacked = unackedMessages(queue).length;
    return { ready: ready, unacked: unacked, total: ready + unacked };
  }

  function addLog(kind, text) {
    const target = state();
    target.log.unshift({ time: nowTime(), kind: kind, text: text });
    target.log = target.log.slice(0, MAX_LOG);
  }

  function toast(message, title) {
    if (typeof window.rlToast === "function") {
      window.rlToast(message, title);
      return;
    }
    let host = $("#sim-toast");
    if (!host) {
      host = document.createElement("div");
      host.id = "sim-toast";
      host.className = "sim-toast";
      document.body.appendChild(host);
    }
    host.innerHTML = "<b>" + esc(title || "rabbit.lab") + "</b><span>" + esc(message) + "</span>";
    host.classList.add("is-visible");
    clearTimeout(host._timer);
    host._timer = setTimeout(() => host.classList.remove("is-visible"), 3600);
  }

  function setRoutePreview(text, tone) {
    const node = $("#sim-route-preview");
    if (!node) return;
    node.textContent = text;
    node.classList.remove("is-ok", "is-bad");
    if (tone) node.classList.add(tone);
  }

  /* ---------- dom / render ---------- */

  const el = {};
  const view = { x: 0, y: 0, scale: 1 };
  const ui = { selected: null, tab: "task", connect: null, toastTimer: null };

  function cacheDom() {
    el.header = $(".sim-header");
    el.sidebar = $(".sim-sidebar");
    el.shell = $(".sim-shell");
    el.projectLabel = $("#sim-project-label");
    el.scenarios = $("#sim-scenarios");
    el.scenarioSelect = $("#sim-scenario-select");
    el.progressText = $("#sim-progress-text");
    el.progressPct = $("#sim-progress-pct");
    el.progressBar = $("#sim-progress-bar");
    el.title = $("#sim-title");
    el.kicker = $("#sim-kicker");
    el.taskCopy = $("#sim-task-copy");
    el.checkCount = $("#sim-check-count");
    el.checkTotal = $("#sim-check-total");
    el.taskSide = $(".sim-task-side");
    el.checkBtn = $("[data-action='check']");
    el.canvas = $("#sim-canvas");
    el.stage = $("#sim-stage");
    el.edges = $("#sim-edges");
    el.nodes = $("#sim-nodes");
    el.zoomLabel = $("#sim-zoom-label");
    el.pauseBtn = $("#sim-pause-btn");
    el.speed = $("#sim-speed");
    el.pubSource = $("#sim-pub-source");
    el.pubExchange = $("#sim-pub-exchange");
    el.pubKey = $("#pub-key");
    el.pubPayload = $("#pub-payload");
    el.pubHeaders = $("#pub-headers");
    el.pubCount = $("#pub-count");
    el.publishBtn = $("#sim-publish-btn");
    el.autosend = $("#sim-autosend");
    el.autosendSec = $("#sim-autosend-sec");
    el.routePreview = $("#sim-route-preview");
    el.inspector = $("#sim-inspector");
    el.tabs = $$("[data-insp-tab]");
    el.panels = $$("[data-panel]");
    el.inspectorContent = $("#sim-inspector-content");
    el.taskTitle = $("#inspector-task-title");
    el.taskDesc = $("#inspector-task-desc");
    el.checks = $("#sim-checks");
    el.taskState = $("#sim-task-state");
    el.hint = $("#sim-hint-text");
    el.health = $("#sim-health");
    el.log = $("#sim-log");
    el.bindingModal = $("#sim-modal-binding");
    el.bindingForm = $("#sim-binding-form");
    el.bindingExchange = $("#binding-exchange");
    el.bindingQueue = $("#binding-queue");
    el.bindingKey = $("#binding-key");
    el.bindingArgs = $("#binding-args");
    el.bindingArgsField = $("#binding-args-field");
    el.bindingKeyNote = $("#sim-binding-key-note");
    el.exportModal = $("#sim-modal-export");
    el.helpModal = $("#sim-modal-help");
    el.importFile = $("#sim-import-file");
    el.metrics = {
      published: $("#metric-published"),
      routed: $("#metric-routed"),
      queued: $("#metric-queued"),
      unacked: $("#metric-unacked"),
      acked: $("#metric-acked"),
      dlx: $("#metric-dlx"),
      dropped: $("#metric-dropped")
    };
  }

  function scenarioOfCurrent() {
    return getScenario(currentId);
  }

  function renderSidebar() {
    const total = GUIDED_SCENARIOS.length;
    const done = GUIDED_SCENARIOS.filter((scenario) => completed.has(scenario.id)).length;
    el.progressText.textContent = done + " / " + total;
    el.progressPct.textContent = Math.round(total ? (done / total) * 100 : 0) + "%";
    el.progressBar.style.width = (total ? (done / total) * 100 : 0) + "%";
    el.scenarios.innerHTML = SCENARIOS.map((scenario) => {
      const active = scenario.id === currentId;
      const isDone = completed.has(scenario.id);
      return '<button type="button" class="sim-scenario' + (active ? " is-active" : "") + (isDone ? " is-done" : "") + '" data-scenario="' + esc(scenario.id) + '" aria-current="' + (active ? "true" : "false") + '">' +
        '<span class="sim-scenario-num">' + esc(scenario.num) + '</span>' +
        '<span><span class="sim-scenario-title">' + esc(scenario.label) + '</span><span class="sim-scenario-sub">' + esc(scenario.sub) + '</span></span>' +
        '<span class="sim-scenario-state">' + (isDone ? "✓" : "") + '</span>' +
        '</button>';
    }).join("");
    el.scenarioSelect.innerHTML = SCENARIOS.map((scenario) =>
      '<option value="' + esc(scenario.id) + '"' + (scenario.id === currentId ? " selected" : "") + '>' + esc(scenario.num + " · " + scenario.label) + '</option>'
    ).join("");
  }

  function renderHeader() {
    const scenario = scenarioOfCurrent();
    el.projectLabel.textContent = scenario.label;
    el.kicker.textContent = scenario.kicker;
    el.title.textContent = scenario.title;
    el.taskCopy.innerHTML = scenario.task;
    el.taskTitle.textContent = scenario.title;
    el.taskDesc.innerHTML = scenario.desc;
    el.hint.textContent = scenario.hint;
    document.title = scenario.title + " — конструктор RabbitMQ | rabbit.lab";
  }

  function renderTask() {
    const scenario = scenarioOfCurrent();
    const results = checkResults();
    const passed = results.filter(Boolean).length;
    if (scenario.checks.length) {
      el.checkCount.textContent = passed;
      el.checkTotal.textContent = scenario.checks.length;
      el.checkBtn.textContent = passed === scenario.checks.length ? "Проверить снова" : "Проверить";
    } else {
      el.checkCount.textContent = "0";
      el.checkTotal.textContent = "0";
      el.checkBtn.textContent = "Аудит схемы";
    }
    el.checks.innerHTML = scenario.checks.length ? scenario.checks.map((check, index) => {
      const ok = results[index];
      return '<div class="sim-check ' + (ok ? "is-pass" : "is-fail") + '"><span class="mark">' + (ok ? "✓" : "×") + '</span><span>' + esc(check.label) + '</span></div>';
    }).join("") : '<div class="sim-empty-state">Свободная схема без заданий. Соберите топологию и нажмите «Аудит схемы»: проверка найдёт обрывы маршрутизации, неверные DLX и пустые bindings.</div>';
    el.taskState.className = "sim-task-state";
    el.taskState.textContent = "";
    if (scenario.checks.length && passed === scenario.checks.length) {
      el.taskState.classList.add("is-visible");
      el.taskState.textContent = scenario.success;
    } else if (state().taskState === "fail") {
      el.taskState.classList.add("is-visible", "is-error");
      el.taskState.textContent = "Пока не всё: проверьте маршрутизацию, сообщения и настройки очередей.";
    }
  }

  function setMetric(node, value, hot, bad) {
    if (!node) return;
    node.textContent = shortNumber(value);
    const box = node.parentElement;
    if (box) {
      box.classList.toggle("is-hot", !!hot);
      box.classList.toggle("is-bad", !!bad);
    }
  }

  function renderMetrics() {
    const target = state();
    let ready = 0;
    let unacked = 0;
    target.queues.forEach((queue) => {
      const counts = queueCounts(queue);
      ready += counts.ready;
      unacked += counts.unacked;
    });
    setMetric(el.metrics.published, target.metrics.published);
    setMetric(el.metrics.routed, target.metrics.routed);
    setMetric(el.metrics.queued, ready, ready > 0);
    setMetric(el.metrics.unacked, unacked, unacked > 0);
    setMetric(el.metrics.acked, target.metrics.acked);
    setMetric(el.metrics.dlx, target.metrics.deadLettered, false, target.metrics.deadLettered > 0);
    setMetric(el.metrics.dropped, target.metrics.dropped + target.metrics.returned, false, (target.metrics.dropped + target.metrics.returned) > 0);
  }

  function nodePorts(kind, id) {
    if (kind === "producer") {
      return '<span class="sim-node-port sim-node-port--out" data-port="out" data-node-kind="producer" data-node-id="' + esc(id) + '" title="Потяните к exchange"></span><span class="sim-node-port-label">out</span>';
    }
    if (kind === "consumer") {
      return '<span class="sim-node-port sim-node-port--in" data-port="in" data-node-kind="consumer" data-node-id="' + esc(id) + '" title="Вход из очереди"></span><span class="sim-node-port-label">in</span>';
    }
    if (kind === "exchange") {
      return '<span class="sim-node-port sim-node-port--out" data-port="out" data-node-kind="exchange" data-node-id="' + esc(id) + '" title="Потяните к очереди"></span><span class="sim-node-port sim-node-port--in" data-port="in" data-node-kind="exchange" data-node-id="' + esc(id) + '" title="Вход от producer"></span>';
    }
    return '<span class="sim-node-port sim-node-port--out" data-port="out" data-node-kind="queue" data-node-id="' + esc(id) + '" title="Потяните к consumer"></span><span class="sim-node-port sim-node-port--in" data-port="in" data-node-kind="queue" data-node-id="' + esc(id) + '" title="Вход от exchange"></span>';
  }

  function bindingChips(target, queue) {
    const bindings = bindingsForQueue(target, queue.id);
    if (!bindings.length) {
      return '<span class="sim-binding sim-binding--add" data-action="add-binding" data-queue="' + esc(queue.id) + '">+ binding</span>';
    }
    return bindings.map((binding) => {
      const exchange = exchangeById(target, binding.exchange);
      const code = binding.args && Object.keys(binding.args).length
        ? "headers " + esc(Object.keys(binding.args).filter((key) => key !== "x-match").join(","))
        : (binding.key || "*");
      return '<span class="sim-binding" title="' + esc((exchange ? exchange.name : binding.exchange) + " → " + code) + '">' + esc(exchange ? exchange.name : binding.exchange) + " · " + code + '</span>';
    }).join("");
  }

  function renderProducerNode(target, producer) {
    const exchange = exchangeById(target, producer.exchangeId);
    const key = producer.defaultKey || target.pub.key;
    return '<div class="sim-node sim-node--producer' + (ui.selected && ui.selected.kind === "producer" && ui.selected.id === producer.id ? " is-selected" : "") + '" data-node-kind="producer" data-node-id="' + esc(producer.id) + '" data-node-key="producer:' + esc(producer.id) + '" tabindex="0" style="left:' + producer.x + 'px;top:' + producer.y + 'px" role="button" aria-label="Producer ' + esc(producer.name) + '">' +
      '<div class="sim-node-head"><span class="sq"></span><span>producer</span><span class="node-kind">app</span></div>' +
      '<div class="sim-node-title">' + esc(producer.name) + '</div>' +
      '<div class="sim-node-meta"><span>to <b>' + esc(exchange ? exchange.name : "—") + '</b></span><span>key ' + esc(key || "*") + '</span><span>pub <b>' + shortNumber(producer.published || 0) + '</b></span></div>' +
      '<button class="sim-node-action" type="button" data-action="publish-node" data-id="' + esc(producer.id) + '">publish →</button>' +
      nodePorts("producer", producer.id) +
      '</div>';
  }

  function renderExchangeNode(target, exchange) {
    const bindings = bindingsForExchange(target, exchange.id);
    const queues = new Set(bindings.map((binding) => binding.queue));
    return '<div class="sim-node sim-node--exchange' + (exchange.kind === "dlx" ? " sim-node--dlx" : "") + (ui.selected && ui.selected.kind === "exchange" && ui.selected.id === exchange.id ? " is-selected" : "") + '" data-node-kind="exchange" data-node-id="' + esc(exchange.id) + '" data-node-key="exchange:' + esc(exchange.id) + '" tabindex="0" style="left:' + exchange.x + 'px;top:' + exchange.y + 'px" role="button" aria-label="Exchange ' + esc(exchange.name) + '">' +
      '<div class="sim-node-head"><span class="sq"></span><span>exchange</span><span class="node-kind">' + esc(exchange.type) + '</span></div>' +
      '<div class="sim-node-title">' + esc(exchange.name) + '</div>' +
      '<div class="sim-node-meta"><span>bindings <b>' + bindings.length + '</b></span><span>queues <b>' + queues.size + '</b></span><span class="' + (exchange.alternateExchange ? "live" : "muted") + '">AE ' + esc(exchange.alternateExchange || "—") + '</span></div>' +
      '<div class="sim-binding-list">' + (bindings.length ? bindings.slice(0, 4).map((binding) => {
        const queue = queueById(target, binding.queue);
        const label = binding.args && Object.keys(binding.args).length
          ? "headers:" + esc(Object.keys(binding.args).filter((key) => key !== "x-match").join(","))
          : (binding.key || "*");
        return '<span class="sim-binding">' + esc(queue ? queue.name : binding.queue) + ' · ' + label + '</span>';
      }).join("") : '<span class="sim-binding sim-binding--add" data-action="add-binding" data-exchange="' + esc(exchange.id) + '">+ binding</span>') + '</div>' +
      nodePorts("exchange", exchange.id) +
      '</div>';
  }

  function renderQueueNode(target, queue) {
    const counts = queueCounts(queue);
    const consumers = consumersForQueue(target, queue.id).filter((consumer) => consumer.active);
    const ttl = queue.ttl ? "ttl " + Math.round(queue.ttl / 100) / 10 + "s" : "";
    const dlx = queue.dlx ? "dlx " + (exchangeById(target, queue.dlx) ? exchangeById(target, queue.dlx).name : "?") : "";
    const isDlq = queue.isDlq || /dlq|dead/i.test(queue.name);
    return '<div class="sim-node sim-node--queue' + (isDlq ? " sim-node--dlq" : "") + (ui.selected && ui.selected.kind === "queue" && ui.selected.id === queue.id ? " is-selected" : "") + '" data-node-kind="queue" data-node-id="' + esc(queue.id) + '" data-node-key="queue:' + esc(queue.id) + '" tabindex="0" style="left:' + queue.x + 'px;top:' + queue.y + 'px" role="button" aria-label="Очередь ' + esc(queue.name) + '">' +
      '<div class="sim-node-head"><span class="sq"></span><span>queue</span><span class="node-kind">' + (consumers.length ? consumers.length + " consumer" : "no consumer") + '</span></div>' +
      '<div class="sim-node-title">' + esc(queue.name) + '</div>' +
      '<div class="sim-node-meta"><span>ready <b class="' + (counts.ready ? "live" : "") + '">' + counts.ready + '</b></span><span>unacked <b class="' + (counts.unacked ? "warn" : "") + '">' + counts.unacked + '</b></span><span>in <b>' + shortNumber(queue.received) + '</b></span>' + (ttl ? '<span class="warn">' + esc(ttl) + '</span>' : "") + (dlx ? '<span>dlx ' + esc(dlx.replace(/^dlx /, "")) + '</span>' : "") + '</div>' +
      '<div class="sim-binding-list">' + bindingChips(target, queue) + '</div>' +
      '<div class="sim-node-add-row"><button class="sim-node-action" type="button" data-action="add-consumer" data-queue="' + esc(queue.id) + '">+ consumer</button><button class="sim-node-action" type="button" data-action="select-node" data-kind="queue" data-id="' + esc(queue.id) + '">props</button></div>' +
      nodePorts("queue", queue.id) +
      '</div>';
  }

  function renderConsumerNode(target, consumer) {
    const queue = queueById(target, consumer.queueId);
    return '<div class="sim-node sim-node--consumer' + (!consumer.active ? " is-off" : "") + (ui.selected && ui.selected.kind === "consumer" && ui.selected.id === consumer.id ? " is-selected" : "") + '" data-node-kind="consumer" data-node-id="' + esc(consumer.id) + '" data-node-key="consumer:' + esc(consumer.id) + '" tabindex="0" style="left:' + consumer.x + 'px;top:' + consumer.y + 'px" role="button" aria-label="Consumer ' + esc(consumer.name) + '">' +
      '<div class="sim-node-head"><span class="sq"></span><span>consumer</span><span class="node-kind">' + (consumer.active ? "online" : "offline") + '</span></div>' +
      '<div class="sim-node-title">' + esc(consumer.name) + '</div>' +
      '<div class="sim-node-meta"><span>queue <b>' + esc(queue ? queue.name : "—") + '</b></span><span>' + (consumer.autoAck ? "auto-ack" : "prefetch " + consumer.prefetch) + '</span><span>got <b>' + shortNumber(consumer.delivered || 0) + '</b></span></div>' +
      '<button class="sim-node-action" type="button" data-action="toggle-consumer" data-id="' + esc(consumer.id) + '">' + (consumer.active ? "pause consumer" : "resume consumer") + '</button>' +
      nodePorts("consumer", consumer.id) +
      '</div>';
  }

  function renderNodes() {
    const target = state();
    el.nodes.innerHTML =
      target.producers.map((producer) => renderProducerNode(target, producer)).join("") +
      target.exchanges.map((exchange) => renderExchangeNode(target, exchange)).join("") +
      target.queues.map((queue) => renderQueueNode(target, queue)).join("") +
      target.consumers.map((consumer) => renderConsumerNode(target, consumer)).join("");
    let maxX = 1100;
    let maxY = 620;
    $$(".sim-node", el.nodes).forEach((node) => {
      maxX = Math.max(maxX, node.offsetLeft + node.offsetWidth + 220);
      maxY = Math.max(maxY, node.offsetTop + node.offsetHeight + 160);
    });
    el.stage.style.width = maxX + "px";
    el.stage.style.height = maxY + "px";
    el.edges.setAttribute("viewBox", "0 0 " + maxX + " " + maxY);
    drawEdges();
  }

  function edgePath(x1, y1, x2, y2) {
    const dx = Math.max(34, Math.abs(x2 - x1) * 0.46);
    return "M " + x1 + " " + y1 + " C " + (x1 + dx) + " " + y1 + ", " + (x2 - dx) + " " + y2 + ", " + x2 + " " + y2;
  }

  function edgeMarkup(path, label, x, y, cls) {
    return '<path class="sim-edge ' + (cls || "") + '" d="' + path + '"></path>' +
      (label ? '<text class="sim-edge-label" x="' + x + '" y="' + (y - 4) + '" text-anchor="middle">' + esc(label) + '</text>' : "");
  }

  function drawEdges() {
    const target = state();
    const parts = [];
    const node = (kind, id) => $('[data-node-key="' + kind + ':' + id + '"]', el.nodes);
    const port = (kind, id, dir) => {
      const box = node(kind, id);
      if (!box) return null;
      return {
        x: box.offsetLeft + (dir === "out" ? box.offsetWidth : 0),
        y: box.offsetTop + box.offsetHeight / 2
      };
    };

    target.producers.forEach((producer) => {
      const from = port("producer", producer.id, "out");
      const to = port("exchange", producer.exchangeId, "in");
      if (from && to) {
        parts.push(edgeMarkup(edgePath(from.x, from.y, to.x, to.y), "publish", (from.x + to.x) / 2, (from.y + to.y) / 2, "sim-edge--producer"));
      }
    });

    const seen = new Set();
    target.bindings.forEach((binding) => {
      const key = binding.exchange + "|" + binding.queue;
      const from = port("exchange", binding.exchange, "out");
      const to = port("queue", binding.queue, "in");
      if (!from || !to) return;
      const queue = queueById(target, binding.queue);
      const exchange = exchangeById(target, binding.exchange);
      const label = binding.args && Object.keys(binding.args).length
        ? "headers · " + Object.keys(binding.args).filter((item) => item !== "x-match").join(",")
        : (exchange && exchange.type === "fanout" ? "fanout" : (binding.key || "*"));
      if (seen.has(key)) {
        parts.push('<path class="sim-edge" d="' + edgePath(from.x, from.y, to.x, to.y) + '"></path>');
      } else {
        seen.add(key);
        parts.push(edgeMarkup(edgePath(from.x, from.y, to.x, to.y), label, (from.x + to.x) / 2, (from.y + to.y) / 2, ""));
      }
      if (queue && queue.dlx) {
        const dlxTo = port("exchange", queue.dlx, "in");
        if (dlxTo) {
          parts.push(edgeMarkup(edgePath(to.x - 12, to.y + 10, dlxTo.x, dlxTo.y + 20), "dead-letter", (to.x + dlxTo.x) / 2, (to.y + dlxTo.y) / 2 + 10, "sim-edge--dlq"));
        }
      }
    });

    target.consumers.forEach((consumer) => {
      const from = port("queue", consumer.queueId, "out");
      const to = port("consumer", consumer.id, "in");
      if (from && to) {
        parts.push(edgeMarkup(edgePath(from.x, from.y, to.x, to.y), consumer.active ? "consume" : "paused", (from.x + to.x) / 2, (from.y + to.y) / 2, "sim-edge--consumer" + (consumer.active ? "" : " is-off")));
      }
    });

    if (ui.connect && ui.connect.draft) {
      parts.push('<path class="sim-edge sim-edge--draft" d="' + edgePath(ui.connect.x1, ui.connect.y1, ui.connect.x2, ui.connect.y2) + '"></path>');
    }
    el.edges.innerHTML = parts.join("");
  }

  function applyView() {
    el.stage.style.transform = "translate(" + view.x + "px," + view.y + "px) scale(" + view.scale + ")";
    el.zoomLabel.textContent = Math.round(view.scale * 100) + "%";
  }

  function rememberView() {
    const target = state();
    target.view = {
      x: view.x,
      y: view.y,
      scale: view.scale,
      w: el.canvas.clientWidth,
      h: el.canvas.clientHeight
    };
  }

  function fitView() {
    const nodes = $$(".sim-node", el.nodes);
    const width = el.canvas.clientWidth;
    const height = el.canvas.clientHeight;
    if (!nodes.length || !width || !height) {
      view.x = 0; view.y = 0; view.scale = 1;
      applyView();
      return;
    }
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    nodes.forEach((node) => {
      minX = Math.min(minX, node.offsetLeft);
      minY = Math.min(minY, node.offsetTop);
      maxX = Math.max(maxX, node.offsetLeft + node.offsetWidth);
      maxY = Math.max(maxY, node.offsetTop + node.offsetHeight);
    });
    const pad = width < 600 ? 34 : 70;
    const boxWidth = Math.max(200, maxX - minX);
    const boxHeight = Math.max(160, maxY - minY);
    const scale = clamp(Math.min((width - pad * 2) / boxWidth, (height - pad * 2) / boxHeight), 0.32, 1.15);
    view.scale = scale;
    view.x = (width - boxWidth * scale) / 2 - minX * scale;
    view.y = (height - boxHeight * scale) / 2 - minY * scale;
    applyView();
    rememberView();
  }

  function centerNode(kind, id) {
    const box = $('[data-node-key="' + kind + ":" + id + '"]', el.nodes);
    if (!box) return;
    const width = el.canvas.clientWidth;
    const height = el.canvas.clientHeight;
    view.x = width / 2 - (box.offsetLeft + box.offsetWidth / 2) * view.scale;
    view.y = height / 2 - (box.offsetTop + box.offsetHeight / 2) * view.scale;
    applyView();
    rememberView();
  }

  function renderPublishSource() {
    const target = state();
    if (!target.producers.length) {
      el.pubSource.innerHTML = '<option value="">нет producer</option>';
      el.pubSource.disabled = true;
      el.pubExchange.textContent = "—";
      el.publishBtn.disabled = true;
      return;
    }
    el.pubSource.disabled = false;
    el.publishBtn.disabled = false;
    if (!producerById(target, target.pub.source)) target.pub.source = target.producers[0].id;
    el.pubSource.innerHTML = target.producers.map((producer) =>
      '<option value="' + esc(producer.id) + '"' + (producer.id === target.pub.source ? " selected" : "") + '>' + esc(producer.name) + '</option>'
    ).join("");
    const producer = producerById(target, target.pub.source);
    const exchange = producer ? exchangeById(target, producer.exchangeId) : null;
    el.pubExchange.textContent = exchange ? exchange.name : "—";
  }

  function syncPublishForm(force) {
    const target = state();
    const active = document.activeElement;
    const keep = !force && (active === el.pubKey || active === el.pubPayload || active === el.pubHeaders || active === el.pubCount);
    if (!keep) {
      if (active !== el.pubKey) el.pubKey.value = target.pub.key || "";
      if (active !== el.pubPayload) el.pubPayload.value = target.pub.payload || "";
      if (active !== el.pubHeaders) el.pubHeaders.value = target.pub.headers || "";
      if (active !== el.pubCount) el.pubCount.value = target.pub.count || 1;
    }
    el.speed.value = String(target.speed || 1);
    el.pauseBtn.textContent = target.paused ? "продолжить" : "пауза";
    el.pauseBtn.classList.toggle("is-active", !!target.paused);
    el.pauseBtn.setAttribute("aria-pressed", target.paused ? "true" : "false");
    renderPublishSource();
  }

  function renderLog() {
    const target = state();
    if (!target.log.length) {
      el.log.innerHTML = '<div class="log-empty">Журнал пуст.</div>';
      return;
    }
    el.log.innerHTML = target.log.slice(0, 80).map((entry) => {
      const cls = entry.kind === "ok" ? "log-line--ok" : entry.kind === "warn" ? "log-line--warn" : entry.kind === "err" ? "log-line--err" : entry.kind === "sys" ? "log-line--sys" : "";
      return '<div class="log-line ' + cls + '"><span class="log-time">' + esc(entry.time) + '</span><span class="log-text">' + esc(entry.text) + '</span></div>';
    }).join("");
  }

  function renderAll(forcePublish) {
    renderSidebar();
    renderHeader();
    renderTask();
    renderMetrics();
    renderNodes();
    renderInspector();
    renderHealth();
    renderLog();
    syncPublishForm(forcePublish);
    if (!state().view) requestAnimationFrame(fitView);
  }

  function renderLive() {
    renderMetrics();
    renderNodes();
    renderTask();
    renderHealth();
    renderLog();
    if (ui.tab === "props" && el.inspector && !el.inspector.contains(document.activeElement)) renderInspector();
  }

  /* ---------- inspector ---------- */

  function setTab(name) {
    ui.tab = name;
    el.tabs.forEach((button) => button.classList.toggle("is-active", button.getAttribute("data-insp-tab") === name));
    el.panels.forEach((panel) => panel.classList.toggle("is-active", panel.getAttribute("data-panel") === name));
    if (el.inspector && (name === "props" || name === "health")) el.inspector.classList.add("is-open");
  }

  function openInspector() {
    if (el.inspector) el.inspector.classList.add("is-open");
    setTab(ui.tab || "task");
  }

  function closeInspector() {
    if (el.inspector) el.inspector.classList.remove("is-open");
  }

  function propInput(kind, id, field, value, type, label, hint) {
    const inputType = type || "text";
    return '<label class="sim-field"><span class="sim-field-label">' + esc(label) + '</span>' +
      '<input type="' + inputType + '" data-kind="' + esc(kind) + '" data-id="' + esc(id) + '" data-field="' + esc(field) + '" value="' + esc(value == null ? "" : value) + '"' + (hint ? ' placeholder="' + esc(hint) + '"' : "") + ' autocomplete="off" spellcheck="false"></label>';
  }

  function propArea(kind, id, field, value, label, hint) {
    return '<label class="sim-field"><span class="sim-field-label">' + esc(label) + '</span>' +
      '<textarea data-kind="' + esc(kind) + '" data-id="' + esc(id) + '" data-field="' + esc(field) + '" placeholder="' + esc(hint || "") + '" spellcheck="false">' + esc(value || "") + '</textarea></label>';
  }

  function propSelect(kind, id, field, value, label, options) {
    return '<label class="sim-field"><span class="sim-field-label">' + esc(label) + '</span><select data-kind="' + esc(kind) + '" data-id="' + esc(id) + '" data-field="' + esc(field) + '">' +
      options.map((option) => '<option value="' + esc(option.value) + '"' + (String(option.value) === String(value) ? " selected" : "") + '>' + esc(option.label) + '</option>').join("") +
      '</select></label>';
  }

  function propCheck(kind, id, field, checked, label) {
    return '<label class="sim-field-inline"><input type="checkbox" data-kind="' + esc(kind) + '" data-id="' + esc(id) + '" data-field="' + esc(field) + '"' + (checked ? " checked" : "") + '> ' + esc(label) + '</label>';
  }

  function inspectorHead(kind, name, desc) {
    const labels = { producer: "producer", exchange: "exchange", queue: "queue", consumer: "consumer" };
    return '<div class="sim-props-head"><div><div class="sim-panel-head">properties / ' + esc(labels[kind] || kind) + '</div><h2 class="inspector-name">' + esc(name) + '</h2></div><span class="kind">' + esc(labels[kind] || kind) + '</span></div>' +
      (desc ? '<p class="sim-note">' + esc(desc) + '</p>' : "");
  }

  function inspectorStats(items) {
    return '<div class="inspector-stats">' + items.map((item) =>
      '<div class="inspector-stat"><b>' + esc(item.value) + '</b><span>' + esc(item.label) + '</span></div>'
    ).join("") + '</div>';
  }

  function removeButton(kind, id) {
    return '<button class="sim-form-actions button is-danger" type="button" data-action="delete-node" data-kind="' + esc(kind) + '" data-id="' + esc(id) + '" style="display:none"></button>';
  }

  function bindingsBlock(kind, node) {
    const target = state();
    const list = kind === "exchange" ? bindingsForExchange(target, node.id) : bindingsForQueue(target, node.id);
    const items = list.map((binding) => {
      const other = kind === "exchange" ? queueById(target, binding.queue) : exchangeById(target, binding.exchange);
      const label = binding.args && Object.keys(binding.args).length
        ? "headers " + Object.keys(binding.args).filter((key) => key !== "x-match").map((key) => key + "=" + binding.args[key]).join(" & ")
        : (binding.key || "*");
      return '<div class="inspector-binding"><span>' + esc(other ? other.name : "?") + ' <code>' + esc(label) + '</code></span><button type="button" data-action="remove-binding" data-id="' + esc(binding.id) + '" title="Удалить binding">×</button></div>';
    }).join("");
    return '<div class="inspector-section"><div class="inspector-section-head"><h3>bindings · ' + list.length + '</h3><button type="button" data-action="add-binding" data-' + (kind === "exchange" ? "exchange" : "queue") + '="' + esc(node.id) + '">+ добавить</button></div>' +
      '<div class="inspector-bindings">' + (items || '<div class="inspector-empty-list">Связей нет. Сообщения не смогут попасть в очередь.</div>') + '</div></div>';
  }

  function renderProducer(target, producer) {
    const exchange = exchangeById(target, producer.exchangeId);
    return inspectorHead("producer", producer.name, producer.desc) +
      inspectorStats([
        { value: shortNumber(producer.published || 0), label: "published" },
        { value: exchange ? exchange.name : "—", label: "exchange" },
        { value: producer.defaultKey || "*", label: "default key" }
      ]) +
      '<div class="sim-form">' +
        propInput("producer", producer.id, "name", producer.name, "text", "имя producer") +
        propSelect("producer", producer.id, "exchangeId", producer.exchangeId, "exchange назначения",
          target.exchanges.length ? target.exchanges.map((item) => ({ value: item.id, label: item.name })) : [{ value: "", label: "нет exchange" }]) +
        propInput("producer", producer.id, "defaultKey", producer.defaultKey, "text", "routing key по умолчанию") +
        propArea("producer", producer.id, "desc", producer.desc, "описание", "например: сервис заказов") +
      '</div>' +
      '<div class="sim-form-actions"><button type="button" data-action="publish-node" data-id="' + esc(producer.id) + '" class="is-accent">publish →</button><button type="button" data-action="delete-node" data-kind="producer" data-id="' + esc(producer.id) + '" class="is-danger">удалить</button></div>';
  }

  function renderExchange(target, exchange) {
    const bindings = bindingsForExchange(target, exchange.id);
    return inspectorHead("exchange", exchange.name, exchange.desc) +
      inspectorStats([
        { value: exchange.type, label: "type" },
        { value: bindings.length, label: "bindings" },
        { value: new Set(bindings.map((binding) => binding.queue)).size, label: "queues" }
      ]) +
      '<div class="sim-form">' +
        propInput("exchange", exchange.id, "name", exchange.name, "text", "имя exchange") +
        propSelect("exchange", exchange.id, "type", exchange.type, "тип", [
          { value: "direct", label: "direct — точный ключ" },
          { value: "fanout", label: "fanout — всем очередям" },
          { value: "topic", label: "topic — маски * и #" },
          { value: "headers", label: "headers — по атрибутам" }
        ]) +
        '<div class="sim-field-row">' + propCheck("exchange", exchange.id, "durable", exchange.durable, "durable") + propCheck("exchange", exchange.id, "autoDelete", exchange.autoDelete, "auto-delete") + '</div>' +
        propCheck("exchange", exchange.id, "internal", exchange.internal, "internal (только для DLX)") +
        propSelect("exchange", exchange.id, "alternateExchange", exchange.alternateExchange || "", "alternate exchange", [{ value: "", label: "нет" }].concat(target.exchanges.filter((item) => item.id !== exchange.id).map((item) => ({ value: item.id, label: item.name })))) +
        propArea("exchange", exchange.id, "desc", exchange.desc, "описание", "назначение exchange") +
      '</div>' +
      bindingsBlock("exchange", exchange) +
      '<div class="sim-form-actions"><button type="button" data-action="add-binding" data-exchange="' + esc(exchange.id) + '" class="is-accent">+ binding</button><button type="button" data-action="delete-node" data-kind="exchange" data-id="' + esc(exchange.id) + '" class="is-danger">удалить</button></div>' +
      '<p class="sim-note">Для fanout routing key игнорируется. Для headers ключ не нужен: сравнение идёт по аргументам binding.</p>';
  }

  function messageMarkup(queue, message) {
    const unacked = message.status === "unacked";
    const consumer = consumerById(state(), message.consumerId);
    const headers = message.headers && Object.keys(message.headers).length
      ? Object.keys(message.headers).map((key) => key + "=" + message.headers[key]).join(" · ")
      : "headers: —";
    return '<div class="queue-message' + (unacked ? " is-unacked" : "") + '">' +
      '<div class="queue-message-head"><span class="queue-message-key">' + esc(message.key || "(без ключа)") + '</span><span>' + esc(message.time || "") + ' · ' + (unacked ? "unacked" : "ready") + '</span></div>' +
      '<div class="queue-message-body">' + esc(message.payload || "") + '</div>' +
      '<div class="sim-message-meta"><span>id ' + esc(String(message.id).slice(-8)) + '</span><span class="' + (unacked ? "unacked" : "ready") + '">' + (unacked ? "consumer " + esc(consumer ? consumer.name : "?") : "ожидает consumer") + '</span><span>' + esc(headers) + '</span>' + (message.redelivered ? '<span>redelivered</span>' : "") + '</div>' +
      '<div class="queue-message-actions">' +
        '<button type="button" data-action="message-ack" data-queue="' + esc(queue.id) + '" data-id="' + esc(message.id) + '">ack</button>' +
        '<button type="button" data-action="message-nack" data-queue="' + esc(queue.id) + '" data-id="' + esc(message.id) + '">nack</button>' +
        '<button type="button" data-action="message-requeue" data-queue="' + esc(queue.id) + '" data-id="' + esc(message.id) + '">requeue</button>' +
        '<button type="button" class="danger" data-action="message-reject" data-queue="' + esc(queue.id) + '" data-id="' + esc(message.id) + '">reject / dlx</button>' +
      '</div></div>';
  }

  function renderQueue(target, queue) {
    const counts = queueCounts(queue);
    const consumers = consumersForQueue(target, queue.id);
    const messages = queue.messages.slice().sort((a, b) => (b.ts || 0) - (a.ts || 0));
    const consumerMarkup = consumers.length ? consumers.map((consumer) =>
      '<div class="consumer-toggle"><span><b>' + esc(consumer.name) + '</b> · ' + (consumer.autoAck ? "auto-ack" : "prefetch " + consumer.prefetch) + ' · got ' + shortNumber(consumer.delivered) + '</span><button type="button" class="switch' + (consumer.active ? " is-on" : "") + '" data-action="toggle-consumer" data-id="' + esc(consumer.id) + '" aria-label="' + (consumer.active ? "Остановить" : "Запустить") + ' consumer"></button></div>'
    ).join("") : '<div class="inspector-empty-list">Consumer не подключён. Сообщения будут накапливаться в ready.</div>';
    return inspectorHead("queue", queue.name, queue.desc) +
      inspectorStats([
        { value: counts.ready, label: "ready" },
        { value: counts.unacked, label: "unacked" },
        { value: shortNumber(queue.received), label: "received" }
      ]) +
      '<div class="sim-form">' +
        propInput("queue", queue.id, "name", queue.name, "text", "имя очереди") +
        '<div class="sim-field-row">' + propInput("queue", queue.id, "ttl", queue.ttl, "number", "TTL, ms", "0 = без TTL") + propInput("queue", queue.id, "maxLength", queue.maxLength, "number", "max length", "0 = без лимита") + '</div>' +
        propSelect("queue", queue.id, "overflow", queue.overflow, "overflow", [
          { value: "drop-head", label: "drop-head — удалить старые" },
          { value: "reject-publish-dlx", label: "reject-publish-dlx — в DLX" }
        ]) +
        propSelect("queue", queue.id, "dlx", queue.dlx || "", "dead-letter exchange", [{ value: "", label: "нет" }].concat(target.exchanges.map((exchange) => ({ value: exchange.id, label: exchange.name })))) +
        propInput("queue", queue.id, "dlRoutingKey", queue.dlRoutingKey, "text", "DL routing key", "пусто = исходный ключ") +
        '<div class="sim-field-row">' + propInput("queue", queue.id, "maxPriority", queue.maxPriority, "number", "max priority", "0 = выкл.") + propCheck("queue", queue.id, "durable", queue.durable, "durable") + '</div>' +
        propCheck("queue", queue.id, "autoDelete", queue.autoDelete, "auto-delete") +
        propArea("queue", queue.id, "desc", queue.desc, "описание", "назначение очереди") +
      '</div>' +
      '<div class="inspector-section"><div class="inspector-section-head"><h3>consumers · ' + consumers.length + '</h3><button type="button" data-action="add-consumer" data-queue="' + esc(queue.id) + '">+ consumer</button></div>' + consumerMarkup + '</div>' +
      bindingsBlock("queue", queue) +
      '<div class="inspector-section"><div class="inspector-section-head"><h3>сообщения · ' + messages.length + '</h3><button type="button" data-action="purge-queue" data-id="' + esc(queue.id) + '">purge</button></div>' +
        '<div class="message-list">' + (messages.length ? messages.slice(0, 30).map((message) => messageMarkup(queue, message)).join("") : '<div class="inspector-empty-list">Очередь пуста.</div>') + '</div></div>' +
      '<div class="sim-form-actions"><button type="button" data-action="add-binding" data-queue="' + esc(queue.id) + '" class="is-accent">+ binding</button><button type="button" data-action="delete-node" data-kind="queue" data-id="' + esc(queue.id) + '" class="is-danger">удалить</button></div>';
  }

  function renderConsumer(target, consumer) {
    const queue = queueById(target, consumer.queueId);
    return inspectorHead("consumer", consumer.name, consumer.desc) +
      inspectorStats([
        { value: consumer.active ? "on" : "off", label: "state" },
        { value: shortNumber(consumer.delivered || 0), label: "delivered" },
        { value: shortNumber(consumer.acked || 0), label: "acked" }
      ]) +
      '<div class="sim-form">' +
        propInput("consumer", consumer.id, "name", consumer.name, "text", "имя consumer") +
        propSelect("consumer", consumer.id, "queueId", consumer.queueId, "очередь", target.queues.length ? target.queues.map((item) => ({ value: item.id, label: item.name })) : [{ value: "", label: "нет очереди" }]) +
        propCheck("consumer", consumer.id, "autoAck", consumer.autoAck, "auto-ack (сразу подтверждать)") +
        propInput("consumer", consumer.id, "prefetch", consumer.prefetch, "number", "prefetch", "сколько unacked держать") +
        propArea("consumer", consumer.id, "desc", consumer.desc, "описание", "например: worker пула") +
      '</div>' +
      '<div class="sim-form-actions"><button type="button" data-action="toggle-consumer" data-id="' + esc(consumer.id) + '" class="is-accent">' + (consumer.active ? "pause" : "resume") + '</button><button type="button" data-action="delete-node" data-kind="consumer" data-id="' + esc(consumer.id) + '" class="is-danger">удалить</button></div>' +
      '<p class="sim-note">Очередь: <b>' + esc(queue ? queue.name : "не найдена") + '</b>. При auto-ack сообщение считается подтверждённым сразу после доставки; при prefetch — остаётся unacked до ack/requeue/reject.</p>';
  }

  function renderInspector() {
    const target = state();
    if (!ui.selected) {
      el.inspectorContent.innerHTML = '<div class="inspector-empty">Выберите узел на схеме — producer, exchange, очередь или consumer. Здесь появятся свойства, bindings и действия с сообщениями.</div>' +
        inspectorStats([
          { value: target.producers.length, label: "producers" },
          { value: target.exchanges.length, label: "exchanges" },
          { value: target.queues.length, label: "queues" }
        ]) +
        '<p class="sim-note">Подсказка: двойной клик по узлу открывает его свойства. Порт на краю узла — начало связи.</p>';
      return;
    }
    const selected = ui.selected;
    let markup = "";
    if (selected.kind === "producer") {
      const producer = producerById(target, selected.id);
      if (producer) markup = renderProducer(target, producer);
    } else if (selected.kind === "exchange") {
      const exchange = exchangeById(target, selected.id);
      if (exchange) markup = renderExchange(target, exchange);
    } else if (selected.kind === "queue") {
      const queue = queueById(target, selected.id);
      if (queue) markup = renderQueue(target, queue);
    } else if (selected.kind === "consumer") {
      const consumer = consumerById(target, selected.id);
      if (consumer) markup = renderConsumer(target, consumer);
    }
    if (!markup) {
      ui.selected = null;
      renderInspector();
      return;
    }
    el.inspectorContent.innerHTML = markup;
  }

  function renderHealth() {
    if (!el.health) return;
    const report = auditTopology();
    const tone = report.score >= 86 ? "" : report.score >= 60 ? "is-warn" : "is-bad";
    const counts = { critical: 0, warning: 0, info: 0 };
    report.issues.forEach((issue) => { counts[issue.severity] = (counts[issue.severity] || 0) + 1; });
    const summary = report.issues.length
      ? "Найдено " + report.issues.length + " замечаний: " + counts.critical + " критичных, " + counts.warning + " предупреждений."
      : "Критичных проблем не найдено. Схема выглядит связной.";
    const list = report.issues.map((issue) => {
      const fix = issue.fix ? '<div class="health-issue-actions"><button type="button" data-action="health-fix" data-fix="' + esc(issue.fix.type) + '" data-kind="' + esc(issue.fix.kind || "") + '" data-id="' + esc(issue.fix.id || "") + '" data-exchange="' + esc(issue.fix.exchange || "") + '" data-queue="' + esc(issue.fix.queue || "") + '">' + esc(issue.fix.label) + '</button></div>' : "";
      return '<div class="health-issue is-' + esc(issue.severity) + '"><div class="health-issue-head"><b>' + esc(issue.title) + '</b><span>' + esc(issue.code) + '</span></div><p>' + esc(issue.text) + '</p>' + fix + '</div>';
    }).join("");
    el.health.innerHTML = '<div class="health-summary"><div class="health-score ' + tone + '">' + report.score + '</div><p><b>' + esc(summary) + '</b><br>' + (report.issues.length ? "Исправьте критичные пункты — они ломают доставку или dead-letter." : "Можно публиковать сообщения и проверять маршрут.") + '</p></div>' +
      (list ? '<div class="health-list">' + list + '</div>' : '<div class="health-ok">✓ bindings на месте<br>✓ DLX указывают на существующие exchange<br>✓ producers и consumers подключены</div>');
  }

  /* ---------- routing engine ---------- */

  function topicMatch(pattern, key) {
    const source = String(pattern || "").split(".");
    const words = String(key == null ? "" : key).split(".");
    const memo = new Map();
    function walk(i, j) {
      const cacheKey = i + ":" + j;
      if (memo.has(cacheKey)) return memo.get(cacheKey);
      let result = false;
      if (i === source.length) {
        result = j === words.length;
      } else if (source[i] === "#") {
        result = walk(i + 1, j) || (j < words.length && walk(i, j + 1));
      } else if (j < words.length && (source[i] === "*" || source[i] === words[j])) {
        result = walk(i + 1, j + 1);
      }
      memo.set(cacheKey, result);
      return result;
    }
    return walk(0, 0);
  }

  function headerMatch(binding, headers) {
    const args = binding.args || {};
    const keys = Object.keys(args).filter((key) => key !== "x-match");
    if (!keys.length) return args["x-match"] === "any" ? false : true;
    const test = (key) => Object.prototype.hasOwnProperty.call(headers, key) && String(headers[key]) === String(args[key]);
    return args["x-match"] === "any" ? keys.some(test) : keys.every(test);
  }

  function bindingMatches(exchange, binding, key, headers) {
    if (exchange.type === "fanout") return true;
    if (exchange.type === "headers") return headerMatch(binding, headers || {});
    if (exchange.type === "topic") return topicMatch(binding.key, key);
    return String(binding.key || "") === String(key == null ? "" : key);
  }

  function computeTargets(target, exchange, key, headers, seen, depth) {
    if (!exchange || (depth || 0) > 8) return [];
    const visited = seen || new Set();
    if (visited.has(exchange.id)) return [];
    visited.add(exchange.id);
    const queues = [];
    bindingsForExchange(target, exchange.id).forEach((binding) => {
      if (!bindingMatches(exchange, binding, key, headers)) return;
      const queue = queueById(target, binding.queue);
      if (queue && queues.indexOf(queue) < 0) queues.push(queue);
    });
    if (!queues.length && exchange.alternateExchange) {
      const alternate = exchangeById(target, exchange.alternateExchange);
      if (alternate) {
        computeTargets(target, alternate, key, headers, visited, (depth || 0) + 1).forEach((queue) => {
          if (queues.indexOf(queue) < 0) queues.push(queue);
        });
      }
    }
    return queues;
  }

  function parseHeaders(text) {
    const value = String(text || "").trim();
    if (!value) return {};
    if (value.charAt(0) === "{") {
      let parsed;
      try {
        parsed = JSON.parse(value);
      } catch (error) {
        throw new Error("headers: невалидный JSON");
      }
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("headers: нужен объект JSON");
      const out = {};
      Object.keys(parsed).forEach((key) => { out[key] = String(parsed[key]); });
      return out;
    }
    const out = {};
    value.split(/\r?\n|;/).forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.charAt(0) === "#") return;
      const index = trimmed.indexOf("=");
      if (index < 1) throw new Error("headers: строка должна быть key=value");
      out[trimmed.slice(0, index).trim()] = trimmed.slice(index + 1).trim();
    });
    return out;
  }

  function deliverToQueue(target, queue, sourceMessage, options) {
    const opts = options || {};
    const ready = readyMessages(queue);
    if (queue.maxLength > 0 && ready.length >= queue.maxLength && queue.overflow === "reject-publish-dlx") {
      queue.dropped += 1;
      if (queue.dlx && !opts.dead) deadLetter(target, queue, sourceMessage, "max-length");
      else target.metrics.dropped += 1;
      addLog("warn", "queue " + queue.name + " · reject-publish · max-length " + queue.maxLength);
      return false;
    }
    const message = Object.assign({}, sourceMessage, {
      id: uid("msg", target.seq++),
      status: "ready",
      consumerId: null,
      deliveredAt: null,
      redelivered: !!sourceMessage.redelivered,
      ts: sourceMessage.ts || Date.now(),
      time: sourceMessage.time || nowTime()
    });
    queue.messages.push(message);
    queue.received += 1;
    queue.seen[message.key || ""] = (queue.seen[message.key || ""] || 0) + 1;
    target.metrics.routed += 1;
    if (queue.maxLength > 0) {
      while (readyMessages(queue).length > queue.maxLength) {
        const oldest = readyMessages(queue)[0];
        queue.messages.splice(queue.messages.indexOf(oldest), 1);
        queue.dropped += 1;
        target.metrics.dropped += 1;
        addLog("warn", "queue " + queue.name + " · drop-head · " + (oldest.key || "(no key)"));
      }
    }
    dispatchQueue(target, queue);
    return true;
  }

  function publishMessage(producer, options) {
    const target = state();
    const opts = options || {};
    const exchange = exchangeById(target, producer.exchangeId);
    if (!exchange || !producer) return { targets: [], exchange: null, producer: producer, key: opts.key || "" };
    const key = opts.key == null ? (producer.defaultKey || "") : opts.key;
    const headers = opts.headers || {};
    const message = {
      id: uid("src", target.seq++),
      key: key,
      payload: opts.payload == null ? target.pub.payload : opts.payload,
      headers: Object.assign({}, headers),
      priority: Number(opts.priority) || 0,
      ts: Date.now(),
      time: nowTime(),
      exchange: exchange.id,
      redelivered: false,
      status: "ready",
      _hops: 0
    };
    target.metrics.published += 1;
    producer.published = (producer.published || 0) + 1;
    const queues = computeTargets(target, exchange, key, headers);
    if (!queues.length) {
      target.metrics.returned += 1;
      addLog("warn", "unroutable · " + exchange.name + " · key " + (key || "(empty)") + " · alternate: " + (exchange.alternateExchange || "none"));
    } else {
      queues.forEach((queue) => {
        if (message._hops < 8) deliverToQueue(target, queue, message);
      });
      addLog("ok", "published · " + exchange.name + " · " + (key || "(no key)") + " → " + queues.map((queue) => queue.name).join(", "));
    }
    target.publishes.unshift({
      key: key,
      exchange: exchange.name,
      targets: queues.map((queue) => queue.name),
      headers: Object.assign({}, headers),
      time: nowTime()
    });
    target.publishes = target.publishes.slice(0, 40);
    return { targets: queues, exchange: exchange, producer: producer, key: key, message: message };
  }

  function deadLetter(target, queue, message, reason) {
    const exchange = exchangeById(target, queue.dlx);
    if (!exchange) {
      target.metrics.dropped += 1;
      queue.dropped += 1;
      addLog("err", "drop · " + queue.name + " · " + reason + " · DLX не настроен");
      return false;
    }
    const hops = (Number(message._hops) || 0) + 1;
    if (hops > 8) {
      target.metrics.dropped += 1;
      queue.dropped += 1;
      addLog("err", "drop · " + queue.name + " · dead-letter loop > 8");
      return false;
    }
    const death = (message.headers && message.headers["x-death"] ? message.headers["x-death"] + " | " : "") + reason + "@" + queue.name;
    const headers = Object.assign({}, message.headers, { "x-death": death });
    const key = queue.dlRoutingKey || message.key || "";
    const next = Object.assign({}, message, {
      headers: headers,
      redelivered: true,
      _hops: hops,
      status: "ready",
      consumerId: null
    });
    target.metrics.deadLettered += 1;
    const targets = computeTargets(target, exchange, key, headers);
    if (!targets.length) {
      target.metrics.dropped += 1;
      addLog("err", "dead-letter · " + queue.name + " → " + exchange.name + " · no binding for " + (key || "(empty)"));
      return false;
    }
    targets.forEach((destination) => deliverToQueue(target, destination, next, { dead: true }));
    addLog("warn", "dead-letter · " + queue.name + " → " + exchange.name + " → " + targets.map((item) => item.name).join(", ") + " · " + reason);
    return true;
  }

  function dispatchQueue(target, queue) {
    let guard = 0;
    while (guard++ < MAX_MESSAGES) {
      const ready = queue.messages.find((message) => message.status !== "unacked");
      if (!ready) break;
      const consumers = target.consumers.filter((consumer) => consumer.queueId === queue.id && consumer.active);
      if (!consumers.length) break;
      let selected = null;
      let selectedIndex = 0;
      for (let offset = 0; offset < consumers.length; offset += 1) {
        const index = ((queue._rr || 0) + offset) % consumers.length;
        const consumer = consumers[index];
        if (consumer.autoAck) {
          selected = consumer;
          selectedIndex = index;
          break;
        }
        const unacked = queue.messages.filter((message) => message.status === "unacked" && message.consumerId === consumer.id).length;
        if (unacked < Math.max(1, Number(consumer.prefetch) || 1)) {
          selected = consumer;
          selectedIndex = index;
          break;
        }
      }
      if (!selected) break;
      queue._rr = (selectedIndex + 1) % consumers.length;
      if (selected.autoAck) {
        queue.messages.splice(queue.messages.indexOf(ready), 1);
        target.metrics.acked += 1;
        selected.delivered = (selected.delivered || 0) + 1;
        selected.acked = (selected.acked || 0) + 1;
        addLog("ok", "consume · " + queue.name + " → " + selected.name + " · auto-ack · " + (ready.key || "(no key)"));
      } else {
        ready.status = "unacked";
        ready.consumerId = selected.id;
        ready.deliveredAt = Date.now();
        selected.delivered = (selected.delivered || 0) + 1;
        addLog("info", "deliver · " + queue.name + " → " + selected.name + " · unacked · prefetch " + selected.prefetch);
      }
    }
  }

  function nackMessage(queue, messageId) {
    const target = state();
    const message = queue.messages.find((item) => item.id === messageId);
    if (!message) return;
    pushHistory();
    queue.messages.splice(queue.messages.indexOf(message), 1);
    queue.nacks += 1;
    target.nacks[queue.name] = (target.nacks[queue.name] || 0) + 1;
    if (queue.dlx) {
      deadLetter(target, queue, message, "rejected");
    } else {
      message.status = "ready";
      message.consumerId = null;
      message.redelivered = true;
      queue.messages.push(message);
      addLog("warn", "nack → requeue · " + queue.name + " · " + (message.key || "(no key)"));
      setTimeout(() => {
        if (states[currentId] === target) {
          dispatchQueue(target, queue);
          renderLive();
          persistSoon();
        }
      }, 700);
    }
    renderLive();
    persistSoon();
  }

  function rejectMessage(queue, messageId) {
    const target = state();
    const message = queue.messages.find((item) => item.id === messageId);
    if (!message) return;
    pushHistory();
    queue.messages.splice(queue.messages.indexOf(message), 1);
    if (queue.dlx) deadLetter(target, queue, message, "rejected");
    else {
      queue.dropped += 1;
      target.metrics.dropped += 1;
      addLog("err", "reject → drop · " + queue.name + " · " + (message.key || "(no key)"));
    }
    dispatchQueue(target, queue);
    renderLive();
    persistSoon();
  }

  function ackMessage(queue, messageId) {
    const target = state();
    const message = queue.messages.find((item) => item.id === messageId);
    if (!message) return;
    pushHistory();
    queue.messages.splice(queue.messages.indexOf(message), 1);
    target.metrics.acked += 1;
    const consumer = consumerById(target, message.consumerId);
    if (consumer) consumer.acked = (consumer.acked || 0) + 1;
    addLog("ok", "ack · " + queue.name + " · " + (message.key || "(no key)") + (consumer ? " · " + consumer.name : ""));
    dispatchQueue(target, queue);
    renderLive();
    persistSoon();
  }

  function requeueMessage(queue, messageId) {
    const target = state();
    const message = queue.messages.find((item) => item.id === messageId);
    if (!message) return;
    pushHistory();
    queue.messages.splice(queue.messages.indexOf(message), 1);
    message.status = "ready";
    message.consumerId = null;
    message.redelivered = true;
    queue.messages.push(message);
    addLog("warn", "requeue · " + queue.name + " · " + (message.key || "(no key)"));
    setTimeout(() => {
      if (states[currentId] === target) {
        dispatchQueue(target, queue);
        renderLive();
        persistSoon();
      }
    }, 600);
    renderLive();
    persistSoon();
  }

  function tickSimulation() {
    if (!booted) return;
    const target = state();
    if (target.paused) return;
    let changed = false;
    target.queues.forEach((queue) => {
      if (queue.ttl > 0) {
        const expired = queue.messages.filter((message) => message.status !== "unacked" && Date.now() - (message.ts || 0) >= queue.ttl);
        expired.forEach((message) => {
          queue.messages.splice(queue.messages.indexOf(message), 1);
          queue.expired += 1;
          target.metrics.expired += 1;
          if (queue.dlx) deadLetter(target, queue, message, "expired");
          else {
            queue.dropped += 1;
            target.metrics.dropped += 1;
            addLog("warn", "ttl expired · " + queue.name + " → drop · " + (message.key || "(no key)"));
          }
          changed = true;
        });
      }
      const count = queue.messages.length;
      dispatchQueue(target, queue);
      if (queue.messages.length !== count) changed = true;
    });
    if (changed) {
      if (!ui.busy) renderLive();
      persistSoon();
    }
  }

  /* ---------- checks ---------- */

  function hasSeen(target, queueName, key) {
    const queue = target.queues.find((item) => item.name === queueName || item.id === queueName);
    if (!queue) return false;
    if (key == null) return Object.keys(queue.seen).length > 0;
    return (queue.seen[key] || 0) > 0;
  }

  function seenCount(target, queueName, key) {
    const queue = target.queues.find((item) => item.name === queueName || item.id === queueName);
    return queue && key == null ? Object.keys(queue.seen).length : (queue ? (queue.seen[key] || 0) : 0);
  }

  function checkResults() {
    const target = state();
    const scenario = scenarioOfCurrent();
    if (!scenario.checks.length) return [];
    const hasPublish = target.metrics.published > 0;
    const hasHeadersPublish = target.publishes.some((item) => item.headers && item.headers.type === "order" && item.headers.region === "eu");
    const queueHasMessages = (name) => {
      const queue = target.queues.find((item) => item.name === name || item.id === name);
      return !!queue && queue.messages.length > 0;
    };
    return scenario.checks.map((check) => {
      switch (check.id) {
        case "pub.direct":
        case "pub.fanout":
        case "pub.topic":
        case "pub.dlq":
        case "pub.retry":
          return hasPublish;
        case "pub.headers":
          return hasHeadersPublish;
        case "queue.orders":
          return hasSeen(target, "orders", "order.created");
        case "queue.audit":
          return hasSeen(target, "audit", null) || hasSeen(target, "audit", "order.created") || hasSeen(target, "audit", "");
        case "queue.payments.none":
          return hasPublish && !hasSeen(target, "payments", null);
        case "queue.email":
          return hasSeen(target, "email", null);
        case "queue.inventory":
          return hasSeen(target, "inventory", null);
        case "queue.analytics":
          return hasSeen(target, "analytics", null);
        case "queue.eu.orders":
          return hasSeen(target, "eu.orders", null);
        case "queue.all.orders":
          return hasSeen(target, "all.orders", null);
        case "queue.us.orders.none":
          return hasPublish && !hasSeen(target, "us.orders", null);
        case "queue.orders.received":
          return hasSeen(target, "orders", "order.created");
        case "nack.orders":
          return (target.nacks.orders || 0) > 0;
        case "queue.orders.empty":
          return (target.nacks.orders || 0) > 0 && !queueHasMessages("orders");
        case "queue.orders.dlq":
          return hasSeen(target, "orders.dlq", null);
        case "nack.work":
          return (target.nacks.work || 0) > 0;
        case "retry.queued":
          return hasSeen(target, "retry", null);
        case "retry.config": {
          const queue = target.queues.find((item) => item.name === "retry");
          return hasPublish && !!queue && queue.ttl > 0 && queue.dlx === "work.direct" && queue.dlRoutingKey === "order.created";
        }
        case "retry.returned":
          return seenCount(target, "work", "order.created") >= 2;
        case "queue.eu.orders.headers":
          return hasSeen(target, "eu.orders", null);
        case "queue.audit.headers":
          return hasSeen(target, "audit", null);
        case "queue.priority.orders.none":
          return hasHeadersPublish && !hasSeen(target, "priority.orders", null);
        default:
          return false;
      }
    });
  }

  function attemptCheck() {
    const scenario = scenarioOfCurrent();
    if (!scenario.checks.length) {
      setTab("health");
      renderHealth();
      toast("Аудит проверяет bindings, DLX, TTL и подключение consumers.", "Свободная схема");
      return;
    }
    const results = checkResults();
    const passed = results.filter(Boolean).length;
    if (passed === scenario.checks.length) {
      const wasDone = completed.has(currentId);
      completed.add(currentId);
      state().taskState = "pass";
      renderSidebar();
      renderTask();
      persistNow(false);
      toast(wasDone ? "Задание уже засчитано. Можно экспериментировать дальше." : scenario.success, "Проверка пройдена · " + passed + "/" + passed);
    } else {
      state().taskState = "fail";
      renderTask();
      persistSoon();
      toast("Пройдено " + passed + " из " + scenario.checks.length + ". " + scenario.hint, "Пока не всё");
    }
  }

  /* ---------- topology audit ---------- */

  function auditTopology() {
    const target = state();
    const issues = [];
    const add = (severity, code, title, text, fix) => issues.push({ severity: severity, code: code, title: title, text: text, fix: fix || null });
    if (!target.exchanges.length && !target.queues.length) {
      add("info", "EMPTY", "Схема пуста", "Добавьте exchange, очередь и producer — или импортируйте definitions.json.", { type: "palette", label: "Открыть палитру" });
    }
    const exNames = {};
    target.exchanges.forEach((exchange) => {
      const duplicate = exNames[exchange.name];
      exNames[exchange.name] = true;
      if (duplicate) add("critical", "DUP-EX", "Дублируется exchange", "Exchange «" + exchange.name + "» объявлен дважды. RabbitMQ не примет такую схему.", { type: "select", label: "Открыть", kind: "exchange", id: exchange.id });
      const bindings = bindingsForExchange(target, exchange.id);
      if (!bindings.length && target.queues.length) add("warning", "EX-UNBOUND", "Exchange без bindings", "В «" + exchange.name + "» не привязана ни одна очередь: сообщения будут возвращаться как unroutable.", { type: "add-binding", label: "+ binding", exchange: exchange.id });
      if (exchange.alternateExchange && !exchangeById(target, exchange.alternateExchange)) add("critical", "AE-MISSING", "Alternate exchange не найден", "У «" + exchange.name + "» указан несуществующий AE. Исправьте имя или создайте exchange.", { type: "select", label: "Открыть", kind: "exchange", id: exchange.id });
      if (exchange.type === "headers") {
        const bad = bindings.find((binding) => !binding.args || !binding.args["x-match"]);
        if (bad) add("warning", "HEADERS-MATCH", "headers binding без x-match", "Binding в «" + exchange.name + "» не задаёт x-match. Добавьте all или any.", { type: "select", label: "Открыть", kind: "exchange", id: exchange.id });
      }
    });
    const queueNames = {};
    target.queues.forEach((queue) => {
      const duplicate = queueNames[queue.name];
      queueNames[queue.name] = true;
      if (duplicate) add("critical", "DUP-Q", "Дублируется очередь", "Очередь «" + queue.name + "» объявлена дважды.", { type: "select", label: "Открыть", kind: "queue", id: queue.id });
      const bindings = bindingsForQueue(target, queue.id);
      if (!bindings.length) add("warning", "Q-UNBOUND", "Очередь без binding", "В «" + queue.name + "» не ведёт ни один binding: сообщения не смогут попасть в очередь.", { type: "add-binding", label: "+ binding", queue: queue.id });
      if (queue.dlx) {
        const dlx = exchangeById(target, queue.dlx);
        if (!dlx) {
          add("critical", "DLX-MISSING", "DLX не найден", "У очереди «" + queue.name + "» указан несуществующий dead-letter exchange.", { type: "select", label: "Исправить", kind: "queue", id: queue.id });
        } else {
          const wildcard = bindingsForExchange(target, dlx.id).find((binding) => /[*#]/.test(binding.key || ""));
          if (dlx.type === "direct" && wildcard) add("critical", "DLX-DIRECT-WILD", "direct DLX с маской", "DLX «" + dlx.name + "» типа direct не понимает * и #. Мёртвые письма не найдутся.", { type: "dlx-topic", label: "Сделать DLX topic", kind: "exchange", id: dlx.id });
          if (dlx.type === "direct" && !queue.dlRoutingKey) {
            const matching = bindingsForExchange(target, dlx.id).some((binding) => binding.key === (queue.dlRoutingKey || ""));
            if (!matching) add("warning", "DLX-KEY", "Не задан DL routing key", "Для direct DLX у «" + queue.name + "» лучше задать x-dead-letter-routing-key: иначе используется исходный ключ.", { type: "select", label: "Открыть", kind: "queue", id: queue.id });
          }
        }
      }
      if (queue.ttl > 0 && !queue.dlx) add("info", "TTL-NO-DLX", "TTL без DLX", "Сообщения из «" + queue.name + "» будут удалены по TTL без dead-letter.", { type: "select", label: "Настроить", kind: "queue", id: queue.id });
      if (queue.maxLength > 0 && queue.overflow === "reject-publish-dlx" && !queue.dlx) add("warning", "MAX-NO-DLX", "reject-publish-dlx без DLX", "При переполнении «" + queue.name + "» сообщение будет потеряно: DLX не задан.", { type: "select", label: "Настроить", kind: "queue", id: queue.id });
      const consumers = consumersForQueue(target, queue.id).filter((consumer) => consumer.active);
      if (!consumers.length && queue.messages.length) add("info", "Q-NO-CONSUMER", "Сообщения без consumer", "В «" + queue.name + "» есть сообщения, но нет активного consumer.", { type: "add-consumer", label: "+ consumer", queue: queue.id });
    });
    target.producers.forEach((producer) => {
      const exchange = exchangeById(target, producer.exchangeId);
      if (!exchange) {
        add("critical", "P-NO-EX", "Producer без exchange", "«" + producer.name + "» не привязан к exchange и не может публиковать.", { type: "select", label: "Открыть", kind: "producer", id: producer.id });
      } else if (!bindingsForExchange(target, exchange.id).length) {
        add("warning", "P-UNROUTABLE", "Producer смотрит в пустой exchange", "Bindings у «" + exchange.name + "» отсутствуют — публикация будет unroutable.", { type: "add-binding", label: "+ binding", exchange: exchange.id });
      }
    });
    target.consumers.forEach((consumer) => {
      if (!queueById(target, consumer.queueId)) add("critical", "C-NO-Q", "Consumer без очереди", "«" + consumer.name + "» не привязан к существующей очереди.", { type: "select", label: "Открыть", kind: "consumer", id: consumer.id });
    });
    const seenBindings = new Set();
    target.bindings.forEach((binding) => {
      const sig = binding.exchange + "|" + binding.queue + "|" + (binding.key || "") + "|" + JSON.stringify(binding.args || {});
      if (seenBindings.has(sig)) add("warning", "DUP-BIND", "Дубликат binding", "Одинаковый binding «" + binding.exchange + " → " + binding.queue + "» объявлен дважды.", { type: "select", label: "Открыть", kind: "queue", id: binding.queue });
      seenBindings.add(sig);
      const exchange = exchangeById(target, binding.exchange);
      if (exchange && exchange.type === "direct" && /[*#]/.test(binding.key || "")) add("warning", "DIRECT-WILD", "Маска в direct binding", "direct не понимает * и #: ключ «" + binding.key + "» не сработает.", { type: "select", label: "Открыть", kind: "exchange", id: exchange.id });
    });
    let score = 100;
    issues.forEach((issue) => { score -= issue.severity === "critical" ? 24 : issue.severity === "warning" ? 8 : 2; });
    return { score: clamp(score, 0, 100), issues: issues };
  }

  /* ---------- history / topology changes ---------- */

  function snapshotTopology() {
    const target = state();
    return clone({
      exchanges: target.exchanges,
      queues: target.queues,
      bindings: target.bindings,
      producers: target.producers,
      consumers: target.consumers,
      seq: target.seq
    });
  }

  function historyBox() {
    if (!histories.has(currentId)) histories.set(currentId, { undo: [], redo: [] });
    return histories.get(currentId);
  }

  function recordHistory(snapshot) {
    const box = historyBox();
    const serialized = JSON.stringify(snapshot);
    if (box.undo.length && JSON.stringify(box.undo[box.undo.length - 1]) === serialized) return;
    box.undo.push(snapshot);
    if (box.undo.length > 30) box.undo.shift();
    box.redo = [];
  }

  function pushHistory() {
    recordHistory(snapshotTopology());
  }

  function restoreTopology(snapshot) {
    const target = state();
    target.exchanges = snapshot.exchanges;
    target.queues = snapshot.queues.map(normalizeQueue);
    target.bindings = snapshot.bindings;
    target.producers = snapshot.producers;
    target.consumers = snapshot.consumers;
    target.seq = snapshot.seq;
    if (ui.selected) {
      const exists = ui.selected.kind === "queue" ? queueById(target, ui.selected.id) : ui.selected.kind === "exchange" ? exchangeById(target, ui.selected.id) : ui.selected.kind === "producer" ? producerById(target, ui.selected.id) : consumerById(target, ui.selected.id);
      if (!exists) ui.selected = null;
    }
  }

  function undo() {
    const box = historyBox();
    if (!box.undo.length) {
      toast("История изменений пуста.", "Undo");
      return;
    }
    box.redo.push(snapshotTopology());
    restoreTopology(box.undo.pop());
    addLog("sys", "undo · topology restored");
    renderAll();
    persistSoon();
  }

  function redo() {
    const box = historyBox();
    if (!box.redo.length) {
      toast("Больше нечего повторять.", "Redo");
      return;
    }
    box.undo.push(snapshotTopology());
    restoreTopology(box.redo.pop());
    addLog("sys", "redo · topology restored");
    renderAll();
    persistSoon();
  }

  function selectNode(kind, id) {
    ui.selected = { kind: kind, id: id };
    setTab("props");
    renderNodes();
    renderInspector();
    openInspector();
  }

  function addNode(kind, x, y) {
    const target = state();
    pushHistory();
    const point = x == null || y == null ? null : { x: Math.round(x), y: Math.round(y) };
    if (kind === "producer") {
      const exchange = mainExchange(target);
      if (!exchange) {
        toast("Сначала создайте exchange — producer должен куда-то публиковать.", "Нужен exchange");
        return;
      }
      const producer = Object.assign(P(uniqueName(target, "producer", "producer"), exchange.id, target.pub.key || "order.created", point ? point.x : 50, point ? point.y : 220), { published: 0, desc: "" });
      producer.id = uid("producer", target.seq++);
      target.producers.push(producer);
      if (!target.pub.source) target.pub.source = producer.id;
      ui.selected = { kind: "producer", id: producer.id };
    } else if (kind === "exchange") {
      const name = uniqueName(target, "exchange", "exchange");
      const exchange = X(name, "direct", point ? point.x : 330, point ? point.y : 220, { desc: "Новый exchange" });
      exchange.id = uid("exchange", target.seq++);
      target.exchanges.push(exchange);
      ui.selected = { kind: "exchange", id: exchange.id };
    } else if (kind === "queue") {
      const name = uniqueName(target, "queue", "queue");
      const queue = normalizeQueue(Q(name, point ? point.x : 660, point ? point.y : 220, { desc: "Новая очередь" }));
      queue.id = uid("queue", target.seq++);
      target.queues.push(queue);
      ui.selected = { kind: "queue", id: queue.id };
    } else if (kind === "consumer") {
      const queue = target.queues[0];
      if (!queue) {
        toast("Сначала создайте очередь — consumer читает из очереди.", "Нужна очередь");
        return;
      }
      const consumer = Object.assign(C(uniqueName(target, "consumer", "consumer"), queue.id, point ? point.x : queue.x + 280, point ? point.y : queue.y, { autoAck: true }), { acked: 0 });
      consumer.id = uid("consumer", target.seq++);
      target.consumers.push(consumer);
      dispatchQueue(target, queue);
      ui.selected = { kind: "consumer", id: consumer.id };
    }
    setTab("props");
    addLog("sys", "node added · " + kind);
    renderAll();
    requestAnimationFrame(() => requestAnimationFrame(fitView));
    persistSoon();
  }

  function confirmAction(text) {
    return window.confirm(text);
  }

  function deleteNode(kind, id) {
    const target = state();
    const node = kind === "queue" ? queueById(target, id) : kind === "exchange" ? exchangeById(target, id) : kind === "producer" ? producerById(target, id) : consumerById(target, id);
    if (!node) return;
    const label = node.name || id;
    if (!confirmAction("Удалить " + kind + " «" + label + "»? Связанные bindings и consumers также будут удалены.")) return;
    pushHistory();
    if (kind === "exchange") {
      target.bindings = target.bindings.filter((binding) => binding.exchange !== id);
      target.queues.forEach((queue) => { if (queue.dlx === id) { queue.dlx = ""; queue.dlRoutingKey = ""; } });
      target.producers.forEach((producer) => { if (producer.exchangeId === id) producer.exchangeId = ""; });
      target.exchanges = target.exchanges.filter((exchange) => exchange.id !== id);
    } else if (kind === "queue") {
      target.bindings = target.bindings.filter((binding) => binding.queue !== id);
      target.consumers = target.consumers.filter((consumer) => consumer.queueId !== id);
      target.queues = target.queues.filter((queue) => queue.id !== id);
    } else if (kind === "producer") {
      target.producers = target.producers.filter((producer) => producer.id !== id);
      if (target.pub.source === id) target.pub.source = target.producers[0] ? target.producers[0].id : "";
    } else if (kind === "consumer") {
      target.consumers = target.consumers.filter((consumer) => consumer.id !== id);
    }
    if (ui.selected && ui.selected.kind === kind && ui.selected.id === id) ui.selected = null;
    addLog("sys", "node deleted · " + kind + " · " + label);
    renderAll();
    persistSoon();
  }

  function addConsumer(queueId) {
    const target = state();
    const queue = queueById(target, queueId);
    if (!queue) return;
    pushHistory();
    const consumer = Object.assign(C(uniqueName(target, "consumer", "consumer"), queue.id, queue.x + 300, queue.y, { autoAck: true }), { acked: 0 });
    consumer.id = uid("consumer", target.seq++);
    target.consumers.push(consumer);
    dispatchQueue(target, queue);
    addLog("sys", "consumer attached · " + consumer.name + " → " + queue.name);
    ui.selected = { kind: "consumer", id: consumer.id };
    setTab("props");
    renderAll();
    persistSoon();
  }

  function toggleConsumer(id) {
    const target = state();
    const consumer = consumerById(target, id);
    if (!consumer) return;
    pushHistory();
    consumer.active = !consumer.active;
    const queue = queueById(target, consumer.queueId);
    if (consumer.active && queue) dispatchQueue(target, queue);
    addLog(consumer.active ? "ok" : "warn", (consumer.active ? "consumer resumed · " : "consumer paused · ") + consumer.name);
    renderAll();
    persistSoon();
  }

  function connectNodes(fromKind, fromId, toKind, toId) {
    const target = state();
    if (fromKind === "producer" && toKind === "exchange") {
      const producer = producerById(target, fromId);
      if (!producer) return false;
      pushHistory();
      producer.exchangeId = toId;
      addLog("sys", "connect · " + producer.name + " → " + (exchangeById(target, toId) || {}).name);
      renderAll();
      persistSoon();
      return true;
    }
    if (fromKind === "exchange" && toKind === "queue") {
      const exchange = exchangeById(target, fromId);
      const queue = queueById(target, toId);
      if (!exchange || !queue) return false;
      const key = exchange.type === "fanout" ? "" : (target.pub.key || "order.created");
      const args = exchange.type === "headers" ? { "x-match": "all", type: "order" } : {};
      const exists = target.bindings.some((binding) => binding.exchange === exchange.id && binding.queue === queue.id && binding.key === key && JSON.stringify(binding.args || {}) === JSON.stringify(args));
      if (exists) {
        toast("Такой binding уже есть.", "Без изменений");
        return false;
      }
      pushHistory();
      target.bindings.push({ id: uid("b", target.seq++), exchange: exchange.id, queue: queue.id, key: key, args: args });
      addLog("ok", "binding · " + exchange.name + " → " + queue.name + " · " + (key || "*"));
      renderAll();
      persistSoon();
      return true;
    }
    if (fromKind === "queue" && toKind === "consumer") {
      const queue = queueById(target, fromId);
      const consumer = consumerById(target, toId);
      if (!queue || !consumer) return false;
      pushHistory();
      consumer.queueId = queue.id;
      dispatchQueue(target, queue);
      addLog("sys", "connect · " + consumer.name + " → " + queue.name);
      renderAll();
      persistSoon();
      return true;
    }
    if (fromKind === "queue" && toKind === "exchange") {
      const queue = queueById(target, fromId);
      const exchange = exchangeById(target, toId);
      if (!queue || !exchange) return false;
      pushHistory();
      queue.dlx = exchange.id;
      addLog("warn", "dead-letter · " + queue.name + " → " + exchange.name);
      renderAll();
      persistSoon();
      return true;
    }
    toast("Так соединять нельзя. Допустимо: producer → exchange, exchange → queue, queue → consumer, queue → DLX exchange.", "Неверная связь");
    return false;
  }

  function updateNode(kind, id, field, value) {
    const target = state();
    const node = kind === "queue" ? queueById(target, id) : kind === "exchange" ? exchangeById(target, id) : kind === "producer" ? producerById(target, id) : consumerById(target, id);
    if (!node) return;
    const trimmed = typeof value === "string" ? value.trim() : value;
    if (field === "name") {
      if (!trimmed) {
        toast("Имя не может быть пустым.", "Не сохранено");
        renderAll();
        return;
      }
      const pool = kind === "queue" ? target.queues : kind === "exchange" ? target.exchanges : kind === "producer" ? target.producers : target.consumers;
      const clash = pool.find((item) => item.id !== id && item.name === trimmed);
      if (clash) {
        toast("Имя «" + trimmed + "» уже занято.", "Не сохранено");
        renderAll();
        return;
      }
    }
    pushHistory();
    if (field === "ttl" || field === "maxLength" || field === "maxPriority" || field === "prefetch") {
      node[field] = Math.max(0, Number(value) || 0);
      if (field === "prefetch" && node[field] < 1) node[field] = 1;
    } else if (field === "durable" || field === "autoDelete" || field === "internal" || field === "autoAck" || field === "active") {
      node[field] = !!value;
    } else {
      node[field] = value;
    }
    if (field === "dlx" && node.dlx === node.id) node.dlx = "";
    if (kind === "consumer" && field === "queueId") {
      const queue = queueById(target, node.queueId);
      if (queue) dispatchQueue(target, queue);
    }
    if (kind === "consumer" && field === "autoAck" && node.autoAck) {
      const queue = queueById(target, node.queueId);
      if (queue) {
        queue.messages.forEach((message) => {
          if (message.status === "unacked" && message.consumerId === node.id) {
            message.status = "ready";
            message.consumerId = null;
            message.redelivered = true;
          }
        });
        dispatchQueue(target, queue);
      }
    }
    if (kind === "queue" && (field === "ttl" || field === "maxLength" || field === "overflow" || field === "dlx")) {
      const queue = queueById(target, id);
      if (queue) dispatchQueue(target, queue);
    }
    addLog("sys", "property changed · " + kind + " " + node.name + " · " + field);
    renderAll();
    persistSoon();
  }

  function removeBinding(id) {
    const target = state();
    const binding = target.bindings.find((item) => item.id === id);
    if (!binding) return;
    pushHistory();
    target.bindings = target.bindings.filter((item) => item.id !== id);
    addLog("warn", "binding removed · " + binding.exchange + " → " + binding.queue);
    renderAll();
    persistSoon();
  }

  function purgeQueue(id) {
    const target = state();
    const queue = queueById(target, id);
    if (!queue) return;
    if (!queue.messages.length) {
      toast("Очередь уже пуста.", queue.name);
      return;
    }
    if (!confirmAction("Очистить очередь «" + queue.name + "»? Все сообщения будут удалены.")) return;
    pushHistory();
    const count = queue.messages.length;
    queue.messages = [];
    addLog("warn", "purge · " + queue.name + " · " + count + " message(s)");
    renderAll();
    persistSoon();
  }

  /* ---------- publish / animation ---------- */

  function nodeBox(kind, id) {
    return $('[data-node-key="' + kind + ":" + id + '"]', el.nodes);
  }

  function flyPacket(from, to, delay, acid) {
    if (!from || !to || state().paused) return;
    const packet = document.createElement("div");
    packet.className = "sim-node-packet" + (acid ? " sim-node-packet--acid" : "");
    el.stage.appendChild(packet);
    const x1 = from.offsetLeft + from.offsetWidth / 2;
    const y1 = from.offsetTop + from.offsetHeight / 2;
    const x2 = to.offsetLeft + to.offsetWidth / 2;
    const y2 = to.offsetTop + to.offsetHeight / 2;
    const duration = Math.max(140, 360 / Math.max(0.5, state().speed || 1));
    const start = performance.now() + (delay || 0);
    function frame(now) {
      if (!packet.parentNode) return;
      if (now < start) {
        requestAnimationFrame(frame);
        return;
      }
      const progress = clamp((now - start) / duration, 0, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      const middle = Math.sin(progress * Math.PI) * Math.min(60, Math.abs(x2 - x1) * 0.12);
      const x = x1 + (x2 - x1) * eased;
      const y = y1 + (y2 - y1) * eased - middle;
      packet.style.left = x + "px";
      packet.style.top = y + "px";
      if (progress < 1) requestAnimationFrame(frame);
      else packet.remove();
    }
    requestAnimationFrame(frame);
  }

  function animateResult(result, options) {
    if (!result || !result.exchange || state().paused) return;
    const opts = options || {};
    const producerBox = result.producer ? nodeBox("producer", result.producer.id) : null;
    const exchangeBox = nodeBox("exchange", result.exchange.id);
    if (producerBox && exchangeBox) flyPacket(producerBox, exchangeBox, 0, false);
    (result.targets || []).forEach((queue, index) => {
      const queueBox = nodeBox("queue", queue.id);
      if (exchangeBox && queueBox && opts.from !== undefined) flyPacket(exchangeBox, queueBox, 70 + index * 45, false);
    });
  }

  function validatePublishInput(target, producer, exchange, key, payload) {
    if (!producer) return "Нет producer: добавьте producer и подключите его к exchange.";
    if (!exchange) return "Producer не подключён к существующему exchange.";
    if (payload && /^[\[{]/.test(payload.trim())) {
      try {
        JSON.parse(payload);
      } catch (error) {
        return "Payload похож на JSON, но не парсится. Исправьте или отправьте обычную строку.";
      }
    }
    if (exchange.type === "direct" && key === "" && !bindingsForExchange(target, exchange.id).some((binding) => binding.key === "")) {
      return "Для direct пустой routing key не совпадёт с bindings.";
    }
    return "";
  }

  function publishFromForm(options) {
    const opts = options || {};
    const target = state();
    const producer = producerSource(target);
    if (!producer) {
      toast("Сначала добавьте producer и exchange.", "Некуда публиковать");
      return;
    }
    const exchange = exchangeById(target, producer.exchangeId);
    const key = el.pubKey.value.trim();
    const payload = el.pubPayload.value;
    const headersText = el.pubHeaders.value;
    let headers;
    try {
      headers = parseHeaders(headersText);
    } catch (error) {
      toast(error.message, "Ошибка headers");
      return;
    }
    const error = validatePublishInput(target, producer, exchange, key, payload);
    if (error) {
      toast(error, "Публикация не выполнена");
      setRoutePreview(error, "is-bad");
      return;
    }
    const count = clamp(Number(el.pubCount.value) || 1, 1, 100);
    target.pub.source = producer.id;
    target.pub.key = key;
    target.pub.payload = payload;
    target.pub.headers = headersText;
    target.pub.count = count;
    let last = null;
    for (let index = 0; index < count; index += 1) {
      last = publishMessage(producer, { key: key, payload: payload, headers: headers });
    }
    animateResult(last, { from: "publish" });
    if (last && last.targets.length) {
      setRoutePreview((key || "(no key)") + " → " + last.targets.map((queue) => queue.name).join(", "), "is-ok");
    } else {
      setRoutePreview((key || "(no key)") + " → unroutable · alternate: " + (exchange && exchange.alternateExchange ? exchange.alternateExchange : "none"), "is-bad");
      toast("Ни одна очередь не совпала. Проверьте тип exchange и bindings.", "Unroutable");
    }
    if (!opts.silent) {
      addLog("sys", "publish batch · " + count + " message(s) · " + producer.name + " → " + (exchange ? exchange.name : "—"));
    }
    state().taskState = "";
    renderLive();
    persistSoon();
  }

  function publishFromProducer(id) {
    const target = state();
    const producer = producerById(target, id);
    if (!producer) return;
    const exchange = exchangeById(target, producer.exchangeId);
    if (!exchange) {
      toast("Producer не подключён к exchange.", "Публикация не выполнена");
      return;
    }
    let headers = {};
    try {
      headers = parseHeaders(target.pub.headers);
    } catch (error) {
      toast(error.message, "Ошибка headers");
      return;
    }
    const key = producer.defaultKey || target.pub.key || "";
    const result = publishMessage(producer, { key: key, payload: target.pub.payload, headers: headers });
    animateResult(result, { from: "node" });
    if (result.targets.length) setRoutePreview((key || "(no key)") + " → " + result.targets.map((queue) => queue.name).join(", "), "is-ok");
    else setRoutePreview((key || "(no key)") + " → unroutable", "is-bad");
    target.taskState = "";
    renderLive();
    persistSoon();
  }

  function traceRoute() {
    const target = state();
    const producer = producerSource(target);
    if (!producer) {
      toast("Добавьте producer.", "Маршрут не построен");
      return;
    }
    const exchange = exchangeById(target, producer.exchangeId);
    if (!exchange) {
      toast("Producer не подключён к exchange.", "Маршрут не построен");
      return;
    }
    const key = el.pubKey.value.trim();
    let headers = {};
    try {
      headers = parseHeaders(el.pubHeaders.value);
    } catch (error) {
      toast(error.message, "Ошибка headers");
      return;
    }
    const queues = computeTargets(target, exchange, key, headers);
    animateResult({ producer: producer, exchange: exchange, targets: queues }, { from: "trace" });
    if (!queues.length) {
      setRoutePreview((key || "(no key)") + " → нет совпадений", "is-bad");
      toast("Маршрут не найден. Проверьте bindings и тип exchange.", "Unroutable");
    } else {
      setRoutePreview((key || "(no key)") + " → " + queues.map((queue) => queue.name).join(", "), "is-ok");
      addLog("info", "trace · " + exchange.name + " · " + (key || "(no key)") + " → " + queues.map((queue) => queue.name).join(", "));
      renderLive();
      persistSoon();
    }
  }

  /* ---------- scenario / simulation state ---------- */

  function stopAutosend() {
    if (autosendTimer) clearInterval(autosendTimer);
    autosendTimer = null;
    if (el.autosend) el.autosend.checked = false;
  }

  function startAutosend() {
    stopAutosend();
    const target = state();
    if (target.paused) {
      toast("Симуляция на паузе. Продолжите, чтобы включить автоотправку.", "Пауза");
      return;
    }
    const seconds = clamp(Number(el.autosendSec.value) || 3, 1, 60);
    el.autosendSec.value = seconds;
    el.autosend.checked = true;
    autosendTimer = setInterval(() => {
      if (!state().paused) publishFromForm({ silent: true });
    }, seconds * 1000);
    addLog("sys", "autosend · every " + seconds + "s");
    renderLog();
    persistSoon();
  }

  function togglePause() {
    const target = state();
    target.paused = !target.paused;
    if (target.paused) stopAutosend();
    addLog("sys", target.paused ? "simulation paused" : "simulation resumed");
    renderLive();
    syncPublishForm(false);
    persistSoon();
  }

  function setSpeed(value) {
    const target = state();
    target.speed = [0.5, 1, 2, 4].indexOf(Number(value)) >= 0 ? Number(value) : 1;
    el.speed.value = String(target.speed);
    addLog("sys", "simulation speed · " + target.speed + "×");
    renderLog();
    persistSoon();
  }

  function loadScenario(id) {
    if (!SCENARIOS.some((scenario) => scenario.id === id)) return;
    stopAutosend();
    currentId = id;
    if (!states[currentId]) states[currentId] = createState(getScenario(currentId));
    ui.selected = null;
    setTab("task");
    closeInspector();
    el.scenarioSelect.value = currentId;
    renderAll(true);
    const target = state();
    const saved = target.view;
    const canvasWidth = el.canvas.clientWidth;
    const canRestore = saved && Number(saved.w) > 0 && Math.abs(Number(saved.w) - canvasWidth) <= Math.max(90, canvasWidth * 0.22);
    if (canRestore) {
      view.x = Number(saved.x) || 0;
      view.y = Number(saved.y) || 0;
      view.scale = clamp(Number(saved.scale) || 1, 0.3, 2);
      applyView();
    } else {
      requestAnimationFrame(() => requestAnimationFrame(fitView));
    }
    try {
      history.replaceState(null, "", "#" + currentId);
    } catch (error) {
      window.location.hash = currentId;
    }
    persistSoon();
  }

  function resetCurrent(force) {
    if (!force && !confirmAction("Сбросить топологию «" + scenarioOfCurrent().label + "»? Сообщения и настройки этой схемы будут потеряны.")) return;
    stopAutosend();
    states[currentId] = createState(getScenario(currentId));
    histories.delete(currentId);
    ui.selected = null;
    setTab("task");
    renderAll(true);
    state().view = null;
    requestAnimationFrame(fitView);
    addLog("sys", "scenario reset · " + currentId);
    renderLog();
    persistNow(false);
  }

  function newEmpty() {
    const free = state();
    if (currentId === "free" && (free.exchanges.length || free.queues.length || free.producers.length)) {
      if (!confirmAction("Очистить свободную схему?")) return;
    }
    stopAutosend();
    states.free = createState(getScenario("free"));
    histories.delete("free");
    currentId = "free";
    ui.selected = null;
    setTab("task");
    renderAll(true);
    requestAnimationFrame(fitView);
    persistNow(false);
  }

  function attemptHealthFix(button) {
    const type = button.getAttribute("data-fix");
    const kind = button.getAttribute("data-kind");
    const id = button.getAttribute("data-id");
    const exchange = button.getAttribute("data-exchange");
    const queue = button.getAttribute("data-queue");
    if (type === "add-binding") {
      openBindingModal({ exchange: exchange, queue: queue });
    } else if (type === "select") {
      selectNode(kind, id);
      centerNode(kind, id);
    } else if (type === "dlx-topic") {
      const ex = exchangeById(state(), id);
      if (!ex) return;
      pushHistory();
      ex.type = "topic";
      addLog("sys", "audit fix · exchange " + ex.name + " → topic");
      renderAll();
      persistSoon();
    } else if (type === "add-consumer") {
      addConsumer(queue);
    } else if (type === "palette") {
      el.sidebar.classList.add("is-open");
      toast("Выберите узел в палитре или перетащите его на холст.", "Палитра");
    }
  }

  /* ---------- modals / binding form ---------- */

  function openModal(modal) {
    if (!modal) return;
    modal.classList.add("is-open");
    modal.setAttribute("aria-hidden", "false");
  }

  function closeModal(modal) {
    if (!modal) return;
    modal.classList.remove("is-open");
    modal.setAttribute("aria-hidden", "true");
  }

  function closeModals() {
    $$(".sim-modal.is-open").forEach((modal) => closeModal(modal));
  }

  function openHelp() {
    openModal(el.helpModal);
  }

  function openExport() {
    try {
      renderExportManifest();
    } catch (error) {
      console.error("export manifest:", error);
    }
    openModal(el.exportModal);
  }

  function openBindingModal(prefill) {
    const target = state();
    const pre = prefill || {};
    if (!target.exchanges.length || !target.queues.length) {
      toast("Для binding нужны минимум один exchange и одна очередь.", "Недостаточно узлов");
      return;
    }
    const preferredQueue = pre.queue || (target.queues[0] && target.queues[0].id);
    let preferredExchange = pre.exchange;
    if (!preferredExchange) {
      const fromQueue = target.bindings.find((binding) => binding.queue === preferredQueue);
      preferredExchange = fromQueue ? fromQueue.exchange : (mainExchange(target) || target.exchanges[0]).id;
    }
    el.bindingExchange.innerHTML = target.exchanges.map((exchange) =>
      '<option value="' + esc(exchange.id) + '"' + (exchange.id === preferredExchange ? " selected" : "") + '>' + esc(exchange.name) + " · " + esc(exchange.type) + '</option>'
    ).join("");
    el.bindingQueue.innerHTML = target.queues.map((queue) =>
      '<option value="' + esc(queue.id) + '"' + (queue.id === preferredQueue ? " selected" : "") + '>' + esc(queue.name) + '</option>'
    ).join("");
    el.bindingKey.value = target.pub.key || "order.created";
    el.bindingArgs.value = '{"x-match":"all","type":"order"}';
    updateBindingForm();
    openModal(el.bindingModal);
    setTimeout(() => { try { el.bindingExchange.focus(); } catch (error) {} }, 50);
  }

  function updateBindingForm() {
    const target = state();
    const exchange = exchangeById(target, el.bindingExchange.value);
    const type = exchange ? exchange.type : "direct";
    const headers = type === "headers";
    el.bindingArgsField.hidden = !headers;
    el.bindingKey.disabled = headers || type === "fanout";
    if (headers) {
      el.bindingKey.value = "";
      el.bindingKey.placeholder = "не используется для headers";
      el.bindingKeyNote.textContent = "Для headers задайте JSON-аргументы: x-match=all или any и пары header=value.";
    } else if (type === "fanout") {
      el.bindingKey.value = "";
      el.bindingKey.placeholder = "fanout игнорирует ключ";
      el.bindingKeyNote.textContent = "Fanout доставляет во все связанные очереди; routing key не нужен.";
    } else if (type === "topic") {
      el.bindingKey.placeholder = "order.*.eu или order.#";
      el.bindingKeyNote.textContent = "Topic: * — ровно одно слово, # — ноль или больше слов.";
    } else {
      el.bindingKey.placeholder = "order.created";
      el.bindingKeyNote.textContent = "Direct: routing key сообщения должен точно совпасть с ключом binding.";
    }
  }

  function submitBinding(event) {
    if (event) event.preventDefault();
    const target = state();
    const exchange = exchangeById(target, el.bindingExchange.value);
    const queue = queueById(target, el.bindingQueue.value);
    if (!exchange || !queue) {
      toast("Выберите exchange и очередь.", "Binding не создан");
      return;
    }
    let key = el.bindingKey.value.trim();
    let args = {};
    if (exchange.type === "headers") {
      try {
        args = JSON.parse(el.bindingArgs.value || "{}");
      } catch (error) {
        toast("Аргументы headers должны быть валидным JSON.", "Ошибка binding");
        return;
      }
      if (!args || typeof args !== "object" || Array.isArray(args) || ["all", "any"].indexOf(args["x-match"]) < 0) {
        toast("Укажите x-match: all или any.", "Ошибка binding");
        return;
      }
      key = "";
    } else if (exchange.type === "fanout") {
      key = "";
    }
    const duplicate = target.bindings.some((binding) => binding.exchange === exchange.id && binding.queue === queue.id && binding.key === key && JSON.stringify(binding.args || {}) === JSON.stringify(args));
    if (duplicate) {
      toast("Такой binding уже существует.", "Без изменений");
      return;
    }
    pushHistory();
    target.bindings.push({ id: uid("b", target.seq++), exchange: exchange.id, queue: queue.id, key: key, args: args });
    addLog("ok", "binding · " + exchange.name + " → " + queue.name + " · " + (key || Object.keys(args).filter((item) => item !== "x-match").join(",") || "*"));
    closeModal(el.bindingModal);
    renderAll();
    persistSoon();
  }

  /* ---------- export ---------- */

  function downloadFile(filename, content, mime) {
    const blob = new Blob([content], { type: mime || "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1200);
  }

  function safeFilename(value) {
    return String(value || "topology").toLowerCase().replace(/[^a-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "topology";
  }

  function queueArguments(queue) {
    const args = {};
    if (queue.ttl > 0) args["x-message-ttl"] = queue.ttl;
    if (queue.maxLength > 0) args["x-max-length"] = queue.maxLength;
    if (queue.overflow && queue.overflow !== "drop-head") args["x-overflow"] = queue.overflow;
    if (queue.dlx) {
      const dlx = exchangeById(state(), queue.dlx);
      if (dlx) args["x-dead-letter-exchange"] = dlx.name;
      if (queue.dlRoutingKey) args["x-dead-letter-routing-key"] = queue.dlRoutingKey;
    }
    if (queue.maxPriority > 0) args["x-max-priority"] = queue.maxPriority;
    return args;
  }

  function buildDefinitions() {
    const target = state();
    return {
      rabbit_version: "3.13.0",
      vhost: "/",
      vhosts: [{ name: "/" }],
      parameters: [],
      global_parameters: [],
      policies: [],
      exchanges: target.exchanges.map((exchange) => {
        const args = {};
        if (exchange.alternateExchange) {
          const alternate = exchangeById(target, exchange.alternateExchange);
          if (alternate) args["alternate-exchange"] = alternate.name;
        }
        return {
          name: exchange.name,
          vhost: "/",
          type: exchange.type,
          durable: !!exchange.durable,
          auto_delete: !!exchange.autoDelete,
          internal: !!exchange.internal,
          arguments: args
        };
      }),
      queues: target.queues.map((queue) => ({
        name: queue.name,
        vhost: "/",
        durable: !!queue.durable,
        auto_delete: !!queue.autoDelete,
        arguments: queueArguments(queue)
      })),
      bindings: target.bindings.map((binding) => {
        const exchange = exchangeById(target, binding.exchange);
        const queue = queueById(target, binding.queue);
        return {
          source: exchange ? exchange.name : binding.exchange,
          vhost: "/",
          destination: queue ? queue.name : binding.queue,
          destination_type: "queue",
          routing_key: binding.key || "",
          arguments: binding.args || {}
        };
      })
    };
  }

  function exportDefinitions() {
    const data = buildDefinitions();
    const name = safeFilename(scenarioOfCurrent().label) + "-definitions.json";
    downloadFile(name, JSON.stringify(data, null, 2), "application/json;charset=utf-8");
    addLog("sys", "export · definitions.json");
    closeModal(el.exportModal);
    renderLog();
    persistSoon();
    toast("definitions.json скачан. Формат совместим с импортом RabbitMQ.", "Экспорт");
  }

  function buildProject() {
    return {
      app: "rabbit.lab",
      version: 2,
      exportedAt: new Date().toISOString(),
      scenario: currentId,
      state: state()
    };
  }

  function exportProject() {
    downloadFile(safeFilename(scenarioOfCurrent().label) + "-project.json", JSON.stringify(buildProject(), null, 2), "application/json;charset=utf-8");
    addLog("sys", "export · rabbit.lab project");
    closeModal(el.exportModal);
    renderLog();
    persistSoon();
  }

  function markdownCell(value) {
    return String(value == null ? "" : value).replace(/\|/g, "\\|").replace(/\r?\n/g, " ");
  }

  function exportMarkdown() {
    const scenario = scenarioOfCurrent();
    downloadFile(safeFilename(scenario.label) + "-topology.md", buildTopologyMarkdown(), "text/markdown;charset=utf-8");
    addLog("sys", "export · topology.md");
    closeModal(el.exportModal);
    renderLog();
    persistSoon();
    toast("Markdown со спецификацией топологии скачан.", "Экспорт");
  }
  /* ---------- import ---------- */

  function importDefinitions(file) {
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const data = JSON.parse(String(reader.result || ""));
        applyImportedData(data);
      } catch (error) {
        toast("Не удалось прочитать definitions.json: " + error.message, "Импорт не выполнен");
      }
    };
    reader.onerror = () => toast("Не удалось прочитать файл.", "Импорт не выполнен");
    reader.readAsText(file);
    el.importFile.value = "";
  }

  function applyProjectData(payload) {
    if (!payload || payload.app !== "rabbit.lab" || !payload.state || typeof payload.state !== "object" || !Array.isArray(payload.state.exchanges) || !Array.isArray(payload.state.queues)) {
      toast("Файл project повреждён: нет app=rabbit.lab или state.", "Импорт не выполнен");
      return;
    }
    const free = states.free;
    if (free && (free.exchanges.length || free.queues.length || free.producers.length)) {
      if (!confirmAction("Импорт заменит свободную схему. Продолжить?")) return;
    }
    const target = normalizeState(getScenario("free"), payload.state);
    target.id = "free";
    target.view = null;
    target.log = [
      { time: nowTime(), kind: "sys", text: "import · rabbit.lab project" },
      { time: nowTime(), kind: "ok", text: "imported · " + target.exchanges.length + " exchange(s) · " + target.queues.length + " queue(s) · " + target.bindings.length + " binding(s)" }
    ].concat(Array.isArray(target.log) ? target.log.slice(0, 6) : []);
    states.free = target;
    currentId = "free";
    histories.delete("free");
    ui.selected = null;
    stopAutosend();
    setTab("health");
    renderAll(true);
    requestAnimationFrame(() => requestAnimationFrame(fitView));
    try { history.replaceState(null, "", "#free"); } catch (error) { window.location.hash = "free"; }
    persistNow(false);
    toast("Проект восстановлен: " + target.exchanges.length + " exchanges, " + target.queues.length + " очередей, " + target.consumers.length + " consumers.", "Готово");
  }

  function applyImportedData(data) {
    if (data && data.app === "rabbit.lab") {
      applyProjectData(data);
      return;
    }
    if (!data || typeof data !== "object" || !Array.isArray(data.exchanges) || !Array.isArray(data.queues)) {
      toast("Ожидается definitions.json или rabbit.lab project.", "Импорт не выполнен");
      return;
    }
    const free = states.free || createState(getScenario("free"));
    if (free.exchanges.length || free.queues.length || free.producers.length) {
      if (!confirmAction("Импорт заменит свободную схему. Продолжить?")) return;
    }
    const target = createState(getScenario("free"));
    target.exchanges = [];
    target.queues = [];
    target.bindings = [];
    target.producers = [];
    target.consumers = [];
    const exByName = new Map();
    const qByName = new Map();
    data.exchanges.forEach((raw, index) => {
      if (!raw || !raw.name) return;
      const name = String(raw.name);
      if (!name) return;
      const allowed = ["direct", "fanout", "topic", "headers"];
      const type = allowed.indexOf(raw.type) >= 0 ? raw.type : "direct";
      const args = raw.arguments && typeof raw.arguments === "object" ? raw.arguments : {};
      const exchange = X(name, type, 310, 130 + index * 170, {
        id: "ex_" + index + "_" + slug(name),
        durable: raw.durable !== false,
        autoDelete: !!raw.auto_delete,
        internal: !!raw.internal,
        desc: "Импортировано из definitions.json"
      });
      if (args["alternate-exchange"]) exchange._alternateName = String(args["alternate-exchange"]);
      target.exchanges.push(exchange);
      if (!exByName.has(name)) exByName.set(name, exchange);
    });
    target.exchanges.forEach((exchange) => {
      exchange.alternateExchange = exchange._alternateName && exByName.has(exchange._alternateName) ? exByName.get(exchange._alternateName).id : "";
      delete exchange._alternateName;
    });
    data.queues.forEach((raw, index) => {
      if (!raw || !raw.name) return;
      const name = String(raw.name);
      const args = raw.arguments && typeof raw.arguments === "object" ? raw.arguments : {};
      const queue = normalizeQueue(Q(name, 660, 90 + index * 145, {
        id: "q_" + index + "_" + slug(name),
        durable: raw.durable !== false,
        autoDelete: !!raw.auto_delete,
        ttl: Math.max(0, Number(args["x-message-ttl"]) || 0),
        maxLength: Math.max(0, Number(args["x-max-length"]) || 0),
        overflow: args["x-overflow"] === "reject-publish-dlx" ? "reject-publish-dlx" : "drop-head",
        maxPriority: Math.max(0, Number(args["x-max-priority"]) || 0),
        desc: "Импортировано из definitions.json"
      }));
      queue._dlxName = args["x-dead-letter-exchange"] ? String(args["x-dead-letter-exchange"]) : "";
      queue.dlRoutingKey = args["x-dead-letter-routing-key"] ? String(args["x-dead-letter-routing-key"]) : "";
      target.queues.push(queue);
      if (!qByName.has(name)) qByName.set(name, queue);
    });
    target.queues.forEach((queue) => {
      queue.dlx = queue._dlxName && exByName.has(queue._dlxName) ? exByName.get(queue._dlxName).id : "";
      delete queue._dlxName;
    });
    (Array.isArray(data.bindings) ? data.bindings : []).forEach((raw, index) => {
      if (!raw || raw.destination_type === "exchange") return;
      const exchange = exByName.get(String(raw.source));
      const queue = qByName.get(String(raw.destination));
      if (!exchange || !queue) return;
      target.bindings.push({
        id: "b_import_" + index,
        exchange: exchange.id,
        queue: queue.id,
        key: raw.routing_key == null ? "" : String(raw.routing_key),
        args: raw.arguments && typeof raw.arguments === "object" ? raw.arguments : {}
      });
    });
    const firstExchange = target.exchanges[0];
    if (firstExchange) {
      const firstBinding = target.bindings[0];
      const producer = P("imported-producer", firstExchange.id, firstBinding ? firstBinding.key : "", 45, 210);
      producer.id = "producer_imported";
      producer.published = 0;
      producer.desc = "Создан при импорте";
      target.producers.push(producer);
      target.pub.source = producer.id;
      target.pub.key = firstBinding ? firstBinding.key : "";
    }
    target.pub.headers = "";
    target.pub.count = 1;
    target.log = [
      { time: nowTime(), kind: "sys", text: "import · definitions.json" },
      { time: nowTime(), kind: "ok", text: "imported · " + target.exchanges.length + " exchange(s) · " + target.queues.length + " queue(s) · " + target.bindings.length + " binding(s)" },
      { time: nowTime(), kind: "sys", text: "consumers и сообщения не импортируются · добавьте вручную" }
    ];
    target.view = null;
    states.free = target;
    currentId = "free";
    histories.delete("free");
    ui.selected = null;
    stopAutosend();
    setTab("health");
    renderAll(true);
    requestAnimationFrame(() => requestAnimationFrame(fitView));
    try { history.replaceState(null, "", "#free"); } catch (error) { window.location.hash = "free"; }
    persistNow(false);
    toast("Импортировано: " + target.exchanges.length + " exchanges, " + target.queues.length + " очередей, " + target.bindings.length + " bindings.", "Готово");
  }


  /* ---------- developer starter kit ---------- */

  const STARTER_ROOT = "rabbit-lab-starter";
  const STARTER_VHOST = "/";
  const STARTER_RABBIT_VERSION = "3.13.0";
  const STARTER_AMQP_URL = "amqp://rabbitlab:rabbitlab-dev@localhost:5672/%2F";

  const STARTER_FILE_META = {
    "README.md": {
      note: "быстрый старт, разбор файлов, чек-лист",
      purpose: "Главная инструкция: как поднять брокер, импортировать топологию, проверить очереди и передать пакет в проект.",
      edit: "Прочитайте полностью. Замените dev-пароли и проверьте production-раздел."
    },
    ".env.example": {
      note: "локальные credentials и порты",
      purpose: "Шаблон переменных окружения для Docker Compose и скриптов. Содержит только dev-значения.",
      edit: "Скопируйте в .env и смените пароль. Не коммитьте .env."
    },
    "docker-compose.yml": {
      note: "RabbitMQ + init-импорт, volume, healthcheck",
      purpose: "Запускает RabbitMQ с management UI, создаёт dev-пользователя и одноразовым init-сервисом импортирует definitions.json; данные сохраняет в volume.",
      edit: "Проверьте версии образов, порты, healthcheck и volume перед production-запуском."
    },
    "rabbitmq/definitions.json": {
      note: "exchanges, queues, bindings, vhost",
      purpose: "Переносимая топология RabbitMQ: exchanges, queues, bindings и аргументы очередей. Без users и паролей.",
      edit: "Обычно не редактируется вручную. Меняйте топологию в rabbit.lab или через API, затем экспортируйте заново."
    },
    "rabbitmq/rabbitmq.conf": {
      note: "listeners, management, без автозагрузки definitions",
      purpose: "Конфигурация брокера: AMQP 5672 и management 15672. Definitions импортируются отдельным init-сервисом после старта.",
      edit: "Для production добавьте TLS, лимиты и настройки кластера."
    },
    "rabbitmq/enabled_plugins": {
      note: "включённый rabbitmq_management",
      purpose: "Явно включает management plugin. Нужен для UI и HTTP API.",
      edit: "Не удаляйте management, если хотите использовать UI и скрипты проверки."
    },
    "topology/topology.md": {
      note: "спецификация схемы, таблицы и маршруты",
      purpose: "Человекочитаемая спецификация: все узлы, аргументы, bindings, consumers и Mermaid-схема.",
      edit: "Используйте для ревью и передачи в команду. При изменении схемы экспортируйте заново."
    },
    "topology/topology.mmd": {
      note: "Mermaid-диаграмма",
      purpose: "Диаграмма потоков producer → exchange → queue → consumer для вставки в Markdown/Confluence.",
      edit: "Можно редактировать как обычный Mermaid-файл."
    },
    "topology/topology.json": {
      note: "канонический снимок структур",
      purpose: "Машиночитаемая модель без сообщений: все узлы, аргументы, runtime-счётчики и пример публикации.",
      edit: "Подходит для скриптов, документации и сравнения окружений."
    },
    "project/rabbit-lab-project.json": {
      note: "импорт обратно в rabbit.lab",
      purpose: "Полный снимок конструктора: позиции, настройки, consumers, история публикаций и сообщения в очередях.",
      edit: "Импортируйте в rabbit.lab. Не публикуйте файл, если в сообщениях есть чувствительные данные."
    },
    "scripts/apply-definitions.ps1": {
      note: "повторный импорт definitions, Windows",
      purpose: "Отправляет definitions.json в management API. Нужен, если definitions изменились после первого старта.",
      edit: "Проверьте URL и credentials; значения берутся из окружения или параметров."
    },
    "scripts/apply-definitions.sh": {
      note: "повторный импорт definitions, bash",
      purpose: "То же для macOS/Linux: curl в /api/definitions с Basic Auth.",
      edit: "Проверьте URL и credentials; скрипт читает .env из корня пакета."
    },
    "scripts/smoke-test.ps1": {
      note: "проверка API, очередей и публикации",
      purpose: "Проверяет management API, наличие очередей и отправляет одно тестовое сообщение в первый exchange.",
      edit: "Запускайте после старта. Скрипт создаёт реальное сообщение в dev-брокере."
    },
    "scripts/smoke-test.sh": {
      note: "smoke-test для macOS/Linux",
      purpose: "Bash-версия проверки API, очередей и маршрутизации тестового сообщения.",
      edit: "Требуется curl. Для имён с пробелами или юникодом проверьте URL-кодирование."
    },
    "apps/README.md": {
      note: "как запустить producer и consumer",
      purpose: "Инструкция по примерам на Node.js: установка, переменные окружения, запуск и ожидаемое поведение.",
      edit: "Адаптируйте команды под вашу ОС и версию Node.js."
    },
    "apps/package.json": {
      note: "amqplib и npm-скрипты",
      purpose: "Минимальный package.json для запуска producer.js и consumer.js. Не является production-шаблоном.",
      edit: "Зафиксируйте версии зависимостей в lock-файле."
    },
    "apps/producer.js": {
      note: "пример confirmed publish",
      purpose: "Публикует JSON с persistent delivery mode, headers и publisher confirms в выбранный exchange.",
      edit: "Замените payload и routing key. Добавьте валидацию и обработку ошибок."
    },
    "apps/consumer.js": {
      note: "пример prefetch / ack / nack",
      purpose: "Читает очередь с ручным подтверждением, prefetch и nack без requeue для демонстрации DLX.",
      edit: "Замените имитацию обработки на реальный handler. Настройте retry/DLX."
    },
    "docs/architecture.md": {
      note: "маршруты, аргументы и ограничения модели",
      purpose: "Техническое описание соответствия rabbit.lab и RabbitMQ, семантики exchange/binding, ack и DLX.",
      edit: "Используйте как основу для ADR и схемы вашего сервиса."
    },
    "docs/operations.md": {
      note: "TLS, backup, monitoring, troubleshooting",
      purpose: "Runbook эксплуатации: безопасность, резервные копии, обновления, мониторинг и типовые ошибки.",
      edit: "Дополните под ваши SLO, секреты и инфраструктуру."
    }
  };

  const STARTER_FOLDER_META = {
    "": {
      title: "rabbit-lab-starter",
      note: "корень пакета и точка входа",
      purpose: "Корневой репозиторий стартового набора: связывает инфраструктуру RabbitMQ, импортируемую топологию, документацию, эксплуатационные скрипты и клиентские примеры в один воспроизводимый артефакт.",
      details: [
        "Корень — это не отдельный сервис, а контракт передачи проекта. Внутри четыре слоя: инфраструктура (docker-compose.yml, rabbitmq/), топология (rabbitmq/definitions.json, topology/), эксплуатация (scripts/, docs/) и клиентский контракт (apps/).",
        "Порядок работы: README → .env → docker compose up → rabbitmq-init → topology.md → smoke-test → apps. Каждый следующий слой опирается на предыдущий, поэтому файлы удобно скачивать по частям именно в этом порядке.",
        "Корень можно скачать целиком как ZIP или раскрыть любой раздел и забрать только нужный файл. Это полезно, когда разработчику нужен только definitions.json, а инфраструктурному инженеру — только Compose и конфиг."
      ],
      tasks: [
        "Передать команде один воспроизводимый пакет вместо набора разрозненных файлов.",
        "Понять, какие артефакты обязательны для запуска, а какие нужны для ревью и эксплуатации.",
        "Скачать весь архив, отдельную папку или один файл под конкретную задачу."
      ],
      edit: "Не меняйте структуру путей без обновления README, Compose, скриптов и ссылок между документами.",
      related: ["README.md", "docker-compose.yml", "rabbitmq/definitions.json", "topology/topology.md", "apps/README.md"]
    },
    "apps/": {
      title: "apps/",
      note: "Node.js примеры producer и consumer",
      purpose: "Показать минимальный рабочий контракт приложения с RabbitMQ: подтверждённая публикация, ручной ack, prefetch и nack без requeue для проверки DLX.",
      details: [
        "Это не библиотека и не production-сервис, а исполняемый пример. Он помогает разработчику проверить, что его приложение использует те же exchange, routing key, очередь и аргументы, которые описаны в topology.",
        "Примеры не объявляют топологию заново: они ожидают, что definitions уже импортированы. Это защищает от случайного изменения аргументов очереди и расхождения между окружениями.",
        "Папка полезна как отправная точка для собственного сервиса: структура подключения, confirms, prefetch и graceful shutdown уже показаны, но production-логирование, метрики и ретраи нужно добавить самостоятельно."
      ],
      tasks: [
        "Запустить producer и увидеть routed=true для текущего routing key.",
        "Запустить consumer и проверить prefetch, ручной ack и nack → DLX.",
        "Взять структуру кода как основу для своего producer/consumer.",
        "Проверить контракт приложения на реальном dev-брокере."
      ],
      edit: "Замените имена exchange, очередей и routing key на свои, добавьте валидацию, логирование, метрики, ретраи и graceful shutdown.",
      related: ["apps/README.md", "apps/producer.js", "apps/consumer.js", "apps/package.json", "rabbitmq/definitions.json"],
      commands: ["cd apps", "npm install", "npm run producer", "npm run consumer"]
    },
    "docs/": {
      title: "docs/",
      note: "архитектура, эксплуатация и ADR",
      purpose: "Объяснить, почему топология устроена именно так, и дать эксплуатационной команде готовый runbook для запуска, проверки и разбора инцидентов.",
      details: [
        "Документация делится на два слоя: architecture.md фиксирует решения и ограничения модели, operations.md описывает повседневные операции, безопасность, backup, monitoring и troubleshooting.",
        "Эти файлы не импортируются в RabbitMQ и не влияют на маршрутизацию. Их задача — сохранить знания между ревью, дежурствами и передачей проекта другой команде.",
        "Если topology.md отвечает на вопрос «что создано», то docs/ отвечает на вопросы «почему так» и «что делать, когда это сломалось»."
      ],
      tasks: [
        "Провести design review топологии и клиентского контракта.",
        "Подготовить runbook для дежурной смены и production-запуска.",
        "Разобрать инцидент с доступом, очередями, DLX или unrouted-сообщениями.",
        "Передать проект другой команде без устных пояснений."
      ],
      edit: "Дополните документацию своими SLO, секретами, инфраструктурой и процедурами реагирования. Не храните здесь реальные пароли.",
      related: ["docs/architecture.md", "docs/operations.md", "topology/topology.md", "README.md"]
    },
    "project/": {
      title: "project/",
      note: "снимок конструктора для обратного импорта",
      purpose: "Сохранить полное состояние rabbit.lab: расположение узлов, настройки, consumers, историю публикаций и сообщения в очередях.",
      details: [
        "Это артефакт симулятора, а не RabbitMQ. Брокер не умеет импортировать такой файл, зато rabbit.lab восстанавливает по нему схему и визуальную раскладку.",
        "В файле могут находиться тестовые сообщения и заголовки. Перед публикацией или передачей во внешнюю систему проверьте содержимое на чувствительные данные.",
        "Project-файл удобно использовать как «снимок перед экспериментом»: скачайте его, измените топологию в симуляторе и при необходимости вернитесь к исходному состоянию."
      ],
      tasks: [
        "Восстановить схему и продолжить редактирование в rabbit.lab.",
        "Сохранить состояние перед рискованным экспериментом.",
        "Передать дизайн коллеге вместе с раскладкой и сообщениями.",
        "Сравнить два состояния проекта."
      ],
      edit: "Импортируйте в rabbit.lab. Не публикуйте файл, если в сообщениях или headers есть секреты и персональные данные.",
      related: ["project/rabbit-lab-project.json", "topology/topology.json", "README.md"]
    },
    "rabbitmq/": {
      title: "rabbitmq/",
      note: "топология, конфиг и плагины брокера",
      purpose: "Инфраструктурный слой RabbitMQ: декларативная топология для импорта, конфигурация узла и список включённых плагинов.",
      details: [
        "definitions.json — переносимый контракт exchanges, queues, bindings и аргументов. rabbitmq.conf — настройки самого узла: listeners и management. enabled_plugins включает API, через который работают UI, rabbitmqadmin и скрипты.",
        "В definitions.json намеренно нет пользователей и паролей. Это позволяет хранить топологию в Git и применять её в разных окружениях, а доступ выдавать отдельно.",
        "Импорт definitions добавляет и обновляет сущности, но не удаляет то, чего нет в файле. Для чистого стенда используйте пустой volume; для существующего — проверяйте diff и права."
      ],
      tasks: [
        "Импортировать exchanges, queues и bindings в RabbitMQ.",
        "Настроить порты, management API и TLS для production.",
        "Проверить, что аргументы DLQ/retry/headers совпадают с документацией.",
        "Диагностировать 401, отсутствующие очереди и ошибки импорта."
      ],
      edit: "definitions.json лучше генерировать из rabbit.lab, а не править вручную. Конфиг и плагины адаптируйте под своё окружение.",
      related: ["rabbitmq/definitions.json", "rabbitmq/rabbitmq.conf", "rabbitmq/enabled_plugins", "docker-compose.yml", "scripts/apply-definitions.ps1"]
    },
    "scripts/": {
      title: "scripts/",
      note: "импорт definitions и smoke-проверка",
      purpose: "Дать повторяемые операции после запуска: применить изменённую топологию и проверить, что management API, очереди и маршрутизация действительно работают.",
      details: [
        "Скрипты импорта отправляют definitions.json в management API с Basic Auth. Они нужны после изменения топологии или при пересоздании тестового стенда.",
        "Smoke-тесты проверяют доступность API, наличие очередей и публикуют одно тестовое сообщение в первый exchange текущего сценария. Это подтверждает routing, но оставляет сообщение в очередях.",
        "PowerShell и Bash-версии делают одно и то же. Выбирайте по ОС и CI-окружению; credentials и URL берите из .env или переменных окружения."
      ],
      tasks: [
        "Повторно применить definitions после изменения схемы.",
        "Проверить стенд после rabbitmq-init или деплоя.",
        "Добавить smoke-проверку в CI как post-deploy шаг.",
        "Диагностировать 401, 404, недоступный exchange и unrouted-сообщения."
      ],
      edit: "Проверьте URL, vhost и credentials. Для production не запускайте smoke-тест в боевые очереди без очистки тестовых сообщений.",
      related: ["scripts/apply-definitions.ps1", "scripts/apply-definitions.sh", "scripts/smoke-test.ps1", "scripts/smoke-test.sh", "rabbitmq/definitions.json"],
      commands: ["./scripts/apply-definitions.ps1", "./scripts/smoke-test.ps1"]
    },
    "topology/": {
      title: "topology/",
      note: "человекочитаемая и машинная спецификация",
      purpose: "Зафиксировать топологию в трёх форматах: Markdown для ревью, Mermaid для диаграмм и JSON для скриптов и сравнения окружений.",
      details: [
        "topology.md объясняет exchanges, queues, bindings, consumers, аргументы и маршруты. topology.mmd даёт готовую диаграмму потоков. topology.json хранит машиночитаемый снимок без сообщений.",
        "Эти файлы не импортируются в RabbitMQ. Источником истины для брокера остаётся rabbitmq/definitions.json, а topology/ помогает проверить и объяснить его содержимое.",
        "При изменении схемы экспортируйте раздел заново: Markdown, Mermaid и JSON должны описывать одну и ту же модель."
      ],
      tasks: [
        "Провести design review маршрутов и аргументов очередей.",
        "Вставить Mermaid-схему в PR, README или Confluence.",
        "Сравнить топологии между сценариями и окружениями.",
        "Передать схему разработчикам без чтения JSON."
      ],
      edit: "Не редактируйте вручную как единственный источник истины. Меняйте топологию в rabbit.lab и экспортируйте раздел заново.",
      related: ["topology/topology.md", "topology/topology.mmd", "topology/topology.json", "rabbitmq/definitions.json"],
      commands: ["./scripts/smoke-test.ps1"]
    }
  };

  const STARTER_FILE_DETAILS = {
    "README.md": {
      details: [
        "README — карта пакета и точка входа. Он объясняет порядок запуска, состав архива, схему импорта definitions, проверку топологии и переход к production-чек-листу.",
        "Файл не участвует в работе RabbitMQ и не импортируется в брокер. Его задача — сократить время до первого запуска и не дать перепутать инфраструктуру, топологию и клиентские примеры.",
        "Если README расходится с фактическими файлами, разработчик теряет время на догадки. Поэтому при изменении структуры пакета обновляйте эту инструкцию вместе с Compose и скриптами."
      ],
      tasks: [
        "Провести нового разработчика от пустого каталога до health/ready за один проход.",
        "Объяснить, почему definitions импортируются через rabbitmq-init, а не management.load_definitions.",
        "Показать, где менять credentials, порты и топологию, и как проверить результат."
      ],
      related: [".env.example", "docker-compose.yml", "rabbitmq/definitions.json", "docs/operations.md"],
      commands: ["docker compose up -d", "docker compose ps -a", "docker compose logs --tail=40 rabbitmq-init"],
      topologyRole: "Собирает инфраструктуру, топологию и клиентский контракт в один маршрут запуска."
    },
    ".env.example": {
      details: [
        "Шаблон локальных переменных: dev-пользователь, пароль, порты AMQP и management, URL для скриптов и приложений.",
        "Compose использует эти значения при создании пустого volume; init-сервис берёт из них credentials для импорта definitions. Секреты не попадают в definitions.json и не должны коммититься в .env.",
        "Если изменить пароль только в .env на существующем volume, RabbitMQ сохранит старые credentials, а импорт и smoke-тест получат 401. Для чистого стенда удалите volume и поднимите его заново."
      ],
      tasks: [
        "Скопировать в .env и заменить dev-пароль до любого общего стенда.",
        "Согласовать порты и URL с локальным окружением.",
        "Не менять vhost: пакет и скрипты рассчитаны на /."
      ],
      related: ["docker-compose.yml", "scripts/apply-definitions.ps1", "apps/README.md"],
      commands: ["Copy-Item .env.example .env", "docker compose up -d"],
      topologyRole: "Задаёт доступ к брокеру и management API, но не саму топологию."
    },
    "docker-compose.yml": {
      details: [
        "Описывает два сервиса: rabbitmq с management UI и volume, и одноразовый rabbitmq-init, который ждёт healthcheck и импортирует definitions через rabbitmqadmin.",
        "Compose создаёт пользователя и vhost на пустом volume, а затем init добавляет exchanges/queues/bindings. Если init завершился с ошибкой, топология не появится, даже если контейнер RabbitMQ healthy.",
        "Healthcheck проверяет сам брокер, а не наличие очередей. Поэтому после docker compose up -d всегда смотрите код выхода rabbitmq-init и запускайте smoke-test."
      ],
      tasks: [
        "Поднять локальный брокер и management UI одной командой.",
        "Проверить статус rabbitmq и код выхода rabbitmq-init.",
        "Настроить порты, volume и healthcheck под своё окружение."
      ],
      related: ["rabbitmq/rabbitmq.conf", "rabbitmq/definitions.json", "scripts/apply-definitions.sh", ".env.example"],
      commands: ["docker compose up -d", "docker compose ps -a", "docker compose logs --tail=80 rabbitmq-init"],
      topologyRole: "Поднимает узел и init-импорт; exchanges/queues появляются из definitions.json."
    },
    "rabbitmq/definitions.json": {
      details: [
        "Переносимый JSON-контракт RabbitMQ: vhost, exchanges, queues, bindings и аргументы очередей (TTL, DLX, max-length, priority, headers).",
        "Здесь нет users и passwords. Это позволяет хранить файл в Git и импортировать его в разные окружения, а доступ выдавать отдельно. Импорт добавляет и обновляет сущности, но не удаляет отсутствующие.",
        "Для текущего сценария файл содержит все exchanges, очереди и bindings, которые вы видите в симуляторе. Если routing не сработал, сначала сравните ключ и binding здесь, а затем проверьте приложение."
      ],
      tasks: [
        "Импортировать топологию в локальный или тестовый RabbitMQ.",
        "Сравнивать окружения и передавать схему между командами.",
        "Проверять, что аргументы DLQ/retry/headers совпадают с topology.md."
      ],
      related: ["topology/topology.md", "topology/topology.json", "scripts/apply-definitions.ps1", "docs/architecture.md"],
      topologyRole: "Главный deployment-артефакт: именно этот файл создаёт exchanges, queues и bindings в брокере."
    },
    "rabbitmq/rabbitmq.conf": {
      details: [
        "Конфигурация узла: AMQP listener 5672 и management HTTP 15672. Файл не описывает exchanges/queues и не содержит credentials.",
        "management.load_definitions здесь намеренно не включён: при автозагрузке RabbitMQ пропускает создание DEFAULT_USER/DEFAULT_VHOST. Импорт делает отдельный init-сервис.",
        "Для production добавьте TLS, лимиты памяти и диска, настройки кластера и политику доступа. Не копируйте dev-порты в открытый интернет."
      ],
      tasks: [
        "Настроить порты и management API.",
        "Добавить TLS, лимиты и cluster-настройки для production.",
        "Не включать автозагрузку definitions без понимания последствий для пользователей."
      ],
      related: ["docker-compose.yml", "rabbitmq/enabled_plugins", "docs/operations.md"],
      topologyRole: "Инфраструктурный слой: включает брокер, но топология появляется только после init-импорта."
    },
    "rabbitmq/enabled_plugins": {
      details: [
        "Список плагинов RabbitMQ. Сейчас включает rabbitmq_management, потому что UI, HTTP API, rabbitmqadmin и скрипты проверки зависят от него.",
        "Если убрать management, контейнер может остаться healthy, но management UI, импорт definitions и smoke-test перестанут работать.",
        "Добавляйте другие плагины осознанно: проверяйте совместимость с версией RabbitMQ и синхронизируйте список с production-конфигурацией."
      ],
      tasks: [
        "Оставить management для локального стенда и проверок.",
        "Добавлять другие плагины осознанно, с проверкой совместимости.",
        "Синхронизировать список с production-конфигурацией."
      ],
      related: ["rabbitmq/rabbitmq.conf", "docker-compose.yml", "scripts/smoke-test.ps1"],
      topologyRole: "Не создаёт топологию, но делает доступными API, через которые она импортируется и проверяется."
    },
    "topology/topology.md": {
      details: [
        "Человекочитаемая спецификация: таблицы exchanges, queues, bindings, consumers, аргументов, маршрутов и Mermaid-диаграмма.",
        "Этот файл не импортируется в RabbitMQ. Он нужен для ревью, документации и объяснения того, почему definitions выглядит именно так.",
        "При изменении схемы проверьте, что Markdown, Mermaid и JSON описывают одну модель. Иначе ревью будет опираться на устаревшую картинку."
      ],
      tasks: [
        "Провести design review топологии до деплоя.",
        "Передать схему в Confluence/README/ADR.",
        "Проверить маршруты и аргументы без чтения JSON."
      ],
      related: ["topology/topology.mmd", "topology/topology.json", "rabbitmq/definitions.json"],
      topologyRole: "Описание текущего сценария: все exchange, очереди и маршруты должны совпадать с definitions.json."
    },
    "topology/topology.mmd": {
      details: [
        "Готовая Mermaid-диаграмма потоков producer → exchange → queue → consumer. Её можно вставить в Markdown или Confluence.",
        "Диаграмма генерируется из текущей модели и не влияет на брокер. При изменении топологии её нужно экспортировать заново.",
        "Это удобный артефакт для созвонов и PR: он показывает маршруты быстрее, чем таблица bindings."
      ],
      tasks: [
        "Вставить схему в PR или документацию.",
        "Показать маршруты и роли очередей на созвоне.",
        "Обновлять вместе с topology.md."
      ],
      related: ["topology/topology.md", "topology/topology.json", "README.md"],
      topologyRole: "Визуальная проекция текущей топологии; не является deployment-файлом."
    },
    "topology/topology.json": {
      details: [
        "Машиночитаемый снимок без сообщений: узлы, аргументы, счётчики, consumers и пример публикации.",
        "Подходит для скриптов, сравнения окружений и проверки, что модель не изменилась неожиданно.",
        "В отличие от project-файла, здесь нет layout и истории сообщений. Это облегчает diff и снижает риск утечки тестовых данных."
      ],
      tasks: [
        "Сравнивать топологии между сценариями или окружениями.",
        "Использовать в автоматических проверках.",
        "Хранить как артефакт ревью вместе с definitions."
      ],
      related: ["topology/topology.md", "rabbitmq/definitions.json", "project/rabbit-lab-project.json"],
      topologyRole: "Канонический машинный снимок текущей модели, но не файл импорта RabbitMQ."
    },
    "project/rabbit-lab-project.json": {
      details: [
        "Полный снимок конструктора: позиции узлов, настройки, consumers, история публикаций и сообщения в очередях.",
        "Он нужен для обратного импорта в rabbit.lab, а не для RabbitMQ. В сообщениях могут быть тестовые или чувствительные данные, поэтому файл нельзя публиковать без проверки.",
        "Если нужен только контракт брокера, берите definitions.json или topology.json: они меньше и не содержат сообщений."
      ],
      tasks: [
        "Восстановить схему в симуляторе и продолжить редактирование.",
        "Передать дизайн коллеге вместе с визуальной раскладкой.",
        "Сохранить состояние перед экспериментами."
      ],
      related: ["topology/topology.json", "README.md"],
      topologyRole: "Снимок модели и UI-состояния; RabbitMQ его не читает."
    },
    "scripts/apply-definitions.ps1": {
      details: [
        "PowerShell-скрипт повторно отправляет definitions.json в management API через Basic Auth. Нужен после изменения топологии или при пересоздании тестового стенда.",
        "Он не создаёт пользователя и не заменяет rabbitmq-init при первом запуске. Если credentials изменились на существующем volume, скрипт получит 401.",
        "Скрипт полезен для Windows-разработчиков и CI на PowerShell. Проверьте URL, vhost и кодирование имён перед запуском."
      ],
      tasks: [
        "Повторно импортировать изменённые definitions на Windows.",
        "Прогнать импорт в CI или локальном PowerShell.",
        "Диагностировать 401 и ошибки management API."
      ],
      related: ["scripts/apply-definitions.sh", "rabbitmq/definitions.json", ".env.example"],
      commands: ["./scripts/apply-definitions.ps1"],
      topologyRole: "Повторно применяет definitions к текущему брокеру."
    },
    "scripts/apply-definitions.sh": {
      details: [
        "Bash-версия импорта definitions: читает .env из корня пакета, кодирует vhost и отправляет JSON через curl.",
        "Полезна на macOS/Linux и в CI. Требует curl и корректный RABBITMQ_MANAGEMENT_URL.",
        "Если management API недоступен, сначала проверьте контейнер и порт 15672, затем credentials и vhost."
      ],
      tasks: [
        "Повторно импортировать топологию на macOS/Linux.",
        "Использовать в shell-пайплайне или контейнере.",
        "Проверить доступность management API перед деплоем."
      ],
      related: ["scripts/apply-definitions.ps1", "rabbitmq/definitions.json", ".env.example"],
      commands: ["./scripts/apply-definitions.sh"],
      topologyRole: "Повторно применяет definitions к текущему брокеру."
    },
    "scripts/smoke-test.ps1": {
      details: [
        "Проверяет management API, наличие очередей и публикует одно тестовое сообщение в первый exchange текущего сценария.",
        "Скрипт подтверждает, что routing работает, но оставляет сообщение в очередях. После проверки можно очистить очереди через API или UI.",
        "Если routed=false, проверьте routing key, тип exchange и bindings. Если 401 — credentials и vhost; если 404 — имя exchange или очереди."
      ],
      tasks: [
        "Проверить стенд после rabbitmq-init.",
        "Убедиться, что routing key совпадает с bindings.",
        "Быстро диагностировать 404/401/недоступный exchange."
      ],
      related: ["scripts/smoke-test.sh", "topology/topology.md", "rabbitmq/definitions.json"],
      commands: ["./scripts/smoke-test.ps1"],
      topologyRole: "Проверяет маршрутизацию в первый exchange текущего сценария."
    },
    "scripts/smoke-test.sh": {
      details: [
        "Bash-версия smoke-test: проверяет API и очереди, публикует сообщение через management API и печатает routed=true/false.",
        "Подходит для macOS/Linux и CI. Требует curl; URL и credentials берутся из .env или переменных окружения.",
        "Тестовое сообщение остаётся в очереди, поэтому для повторных прогонов предусмотрите очистку или отдельный dev-стенд."
      ],
      tasks: [
        "Проверить стенд в shell-окружении.",
        "Добавить в CI как post-deploy проверку.",
        "Понять, почему сообщение не маршрутизируется."
      ],
      related: ["scripts/smoke-test.ps1", "topology/topology.md", "rabbitmq/definitions.json"],
      commands: ["./scripts/smoke-test.sh"],
      topologyRole: "Проверяет маршрутизацию в первый exchange текущего сценария."
    },
    "apps/README.md": {
      details: [
        "Инструкция по примерам: установка amqplib, переменные EXCHANGE/ROUTING_KEY/QUEUE/PREFETCH, запуск producer и consumer.",
        "Примеры не объявляют топологию и не заменяют definitions. Они подключаются к уже импортированному брокеру.",
        "Если приложение использует другие имена, чем topology, сообщения будут unrouted или consumer не найдёт очередь. Сверяйте переменные с текущей схемой."
      ],
      tasks: [
        "Запустить первый producer/consumer на Node.js.",
        "Понять, как приложение использует текущую топологию.",
        "Адаптировать переменные под свой сервис."
      ],
      related: ["apps/producer.js", "apps/consumer.js", "apps/package.json", "topology/topology.md"],
      commands: ["cd apps", "npm install", "npm run producer", "npm run consumer"],
      topologyRole: "Объясняет, как примеры используют текущие exchange, queue и routing key."
    },
    "apps/package.json": {
      details: [
        "Минимальный package.json с amqplib и командами producer/consumer. Версии не зафиксированы lock-файлом.",
        "Это пример зависимостей, а не production-манифест. Для реального сервиса добавьте lock-файл, линтер, тесты и политику обновлений.",
        "Node.js должен быть версии 20 или новее, как указано в engines."
      ],
      tasks: [
        "Установить amqplib для примеров.",
        "Запустить producer/consumer через npm-скрипты.",
        "Зафиксировать версии перед передачей в CI."
      ],
      related: ["apps/README.md", "apps/producer.js", "apps/consumer.js"],
      commands: ["cd apps", "npm install"],
      topologyRole: "Зависимости клиентских примеров; топологию не описывает."
    },
    "apps/producer.js": {
      details: [
        "Пример подтверждённой публикации: persistent delivery mode, JSON payload, headers, publisher confirms и вывод routed=true/false.",
        "Код использует exchange и routing key текущего сценария. Это хорошая отправная точка для собственного producer, но без production-логирования и ретраев.",
        "Если routed=false, сообщение не попало ни в одну очередь. Проверьте binding и routing key, а не только подключение к брокеру."
      ],
      tasks: [
        "Проверить, что сообщение принимает exchange.",
        "Посмотреть publisher confirms и routed.",
        "Заменить payload и routing key на реальные."
      ],
      related: ["apps/consumer.js", "apps/README.md", "topology/topology.md"],
      commands: ["cd apps", "npm run producer"],
      topologyRole: "Публикует в exchange текущего сценария с выбранным routing key."
    },
    "apps/consumer.js": {
      details: [
        "Пример consumer с ручным ack, prefetch и nack без requeue. При nack сообщение уходит в DLX, если очередь это настроила.",
        "Это демонстрация контракта обработки, а не готовый worker: нет ретраев, дедупликации, метрик и production graceful shutdown.",
        "Проверьте, что очередь и prefetch совпадают с текущей топологией, иначе поведение будет отличаться от ожидаемого."
      ],
      tasks: [
        "Проверить prefetch и ручное подтверждение.",
        "Воспроизвести nack → DLX/retry.",
        "Заменить имитацию обработки на реальный handler."
      ],
      related: ["apps/producer.js", "topology/topology.md", "docs/architecture.md"],
      commands: ["cd apps", "npm run consumer"],
      topologyRole: "Читает очередь текущего сценария с ручным ack и prefetch."
    },
    "docs/architecture.md": {
      details: [
        "Техническое объяснение соответствия rabbit.lab и RabbitMQ: exchange/binding, аргументы, ack/nack, DLX, TTL, retry и ограничения модели.",
        "Файл нужен для ADR и ревью. Он не меняет брокер, но фиксирует, почему топология и клиентский контракт устроены именно так.",
        "Используйте его вместе с topology.md: первый объясняет семантику, второй показывает конкретные объекты и маршруты."
      ],
      tasks: [
        "Зафиксировать архитектурные решения по топологии.",
        "Объяснить разработчику разницу между definitions и поведением приложения.",
        "Использовать как основу для ADR."
      ],
      related: ["docs/operations.md", "topology/topology.md", "rabbitmq/definitions.json"],
      topologyRole: "Объясняет семантику текущей топологии и ограничения симулятора."
    },
    "docs/operations.md": {
      details: [
        "Runbook: первый запуск, ежедневные проверки, пользователи и права, TLS, backup, monitoring, обновления и troubleshooting.",
        "Содержит команды для диагностики init, 401, отсутствующих очередей и unrouted-сообщений. Адаптируйте под свои SLO и инфраструктуру.",
        "Не храните в документе реальные пароли и ключи. Ссылайтесь на секрет-хранилище и переменные окружения."
      ],
      tasks: [
        "Проверить стенд после деплоя.",
        "Разобрать инцидент с очередями или доступом.",
        "Подготовить production-runbook."
      ],
      related: ["README.md", "docs/architecture.md", "scripts/smoke-test.sh"],
      commands: ["docker compose ps -a", "docker compose logs --tail=80 rabbitmq-init"],
      topologyRole: "Описывает эксплуатацию стенда и диагностику текущей топологии."
    }
  };

  function starterBool(value) {
    return value ? "yes" : "no";
  }

  function starterCode(value) {
    return "`" + String(value == null ? "" : value).replace(/`/g, "\\`") + "`";
  }

  function starterFence(value, language) {
    const text = String(value == null ? "" : value);
    let fence = "```";
    while (text.indexOf(fence) >= 0) fence += "`";
    return fence + (language || "") + "\n" + text + "\n" + fence;
  }

  function starterYaml(value) {
    return JSON.stringify(String(value == null ? "" : value));
  }

  function starterPs(value) {
    return "'" + String(value == null ? "" : value).replace(/'/g, "''") + "'";
  }

  function starterSh(value) {
    return '"' + String(value == null ? "" : value)
      .replace(/\\/g, "\\\\")
      .replace(/"/g, '\\"')
      .replace(/\$/g, "\\$")
      .replace(/`/g, "\\`") + '"';
  }

  function starterDate(value) {
    const date = value instanceof Date ? value : new Date(value);
    try {
      return new Intl.DateTimeFormat("ru-RU", { dateStyle: "long", timeStyle: "short" }).format(date);
    } catch (error) {
      return date.toISOString();
    }
  }

  function starterArgs(args) {
    const entries = Object.keys(args || {});
    if (!entries.length) return "—";
    return entries.map((key) => key + "=" + String(args[key])).join(", ");
  }

  function starterHeaders(ctx) {
    const entries = Object.keys(ctx.headers || {});
    if (!entries.length) return "—";
    return entries.map((key) => key + "=" + String(ctx.headers[key])).join(", ");
  }

  function starterParseHeaders(text) {
    try {
      return parseHeaders(text || "");
    } catch (error) {
      return {};
    }
  }

  function starterContext() {
    const target = state();
    const scenario = scenarioOfCurrent();
    const firstProducer = target.producers.find((producer) => exchangeById(target, producer.exchangeId)) || target.producers[0] || null;
    let exchange = firstProducer ? exchangeById(target, firstProducer.exchangeId) : null;
    if (!exchange) exchange = target.exchanges[0] || null;
    const firstConsumer = target.consumers.find((consumer) => queueById(target, consumer.queueId)) || target.consumers[0] || null;
    let queue = firstConsumer ? queueById(target, firstConsumer.queueId) : null;
    if (!queue) queue = target.queues[0] || null;
    const headers = starterParseHeaders(target.pub && target.pub.headers);
    const rawPayload = String((target.pub && target.pub.payload) || "");
    let prettyPayload = rawPayload;
    try {
      prettyPayload = JSON.stringify(JSON.parse(rawPayload), null, 2);
    } catch (error) {
      prettyPayload = rawPayload;
    }
    const routingKey = (target.pub && target.pub.key) || (firstProducer && firstProducer.defaultKey) || "";
    const source = firstProducer ? firstProducer.name : "publisher";
    return {
      target: target,
      scenario: scenario,
      generatedAt: new Date(),
      exchange: exchange,
      exchangeName: exchange ? exchange.name : "",
      exchangeType: exchange ? exchange.type : "direct",
      queue: queue,
      queueName: queue ? queue.name : "",
      producer: firstProducer,
      consumer: firstConsumer,
      producerName: firstProducer ? firstProducer.name : "publisher",
      consumerName: firstConsumer ? firstConsumer.name : "consumer",
      routingKey: routingKey,
      headers: headers,
      payload: rawPayload,
      prettyPayload: prettyPayload,
      source: source,
      prefetch: firstConsumer ? Math.max(1, Number(firstConsumer.prefetch) || 1) : 10,
      autoAck: !!(firstConsumer && firstConsumer.autoAck),
      amqpUrl: STARTER_AMQP_URL
    };
  }

  function starterIsDlq(target, queue) {
    return !!(queue && (queue.isDlq || /dlq|dead/i.test(queue.name)));
  }

  function starterConsumerState(consumer) {
    if (!consumer) return "нет consumer";
    if (!consumer.active) return "paused";
    return "running";
  }

  function starterBindingSemantics(binding, exchange) {
    if (!exchange) return "exchange не найден";
    if (exchange.type === "fanout") return "fanout: ключ игнорируется, важна связь";
    if (exchange.type === "headers") {
      const mode = binding.args && binding.args["x-match"] ? binding.args["x-match"] : "all";
      return "headers: x-match=" + mode + ", совпадение заголовков";
    }
    if (exchange.type === "topic") return "topic: маска " + (binding.key || "#");
    return "direct: точное совпадение ключа";
  }

  function starterQueueCounts(queue) {
    const messages = Array.isArray(queue.messages) ? queue.messages : [];
    return {
      ready: messages.filter((message) => message.status !== "unacked").length,
      unacked: messages.filter((message) => message.status === "unacked").length,
      total: messages.length
    };
  }

  function starterQueueArgRows(target) {
    const rows = [];
    const seen = {};
    target.queues.forEach((queue) => {
      const args = queueArguments(queue);
      Object.keys(args).forEach((key) => {
        const signature = key + "=" + String(args[key]);
        if (seen[signature]) {
          seen[signature].queues.push(queue.name);
          return;
        }
        seen[signature] = { key: key, value: args[key], queues: [queue.name] };
        rows.push(seen[signature]);
      });
    });
    return rows;
  }

  const STARTER_ARG_MEANINGS = {
    "x-message-ttl": "Сообщение живёт в очереди не дольше указанного времени (мс), затем истекает.",
    "x-max-length": "Максимальное количество ready-сообщений в очереди; лишние удаляются согласно x-overflow.",
    "x-overflow": "Поведение при переполнении: drop-head удаляет старые, reject-publish отклоняет новые.",
    "x-dead-letter-exchange": "Exchange, в который уходят истёкшие, отклонённые или вытесненные сообщения.",
    "x-dead-letter-routing-key": "Ключ, с которым сообщение перепубликуется в dead-letter exchange.",
    "x-max-priority": "Включает приоритеты сообщений в классической очереди; большее число — выше приоритет."
  };

  function buildTopologyModel() {
    const target = state();
    const scenario = scenarioOfCurrent();
    return {
      schema: "rabbit.lab/topology@1",
      generator: "rabbit.lab simulator v2",
      generatedAt: new Date().toISOString(),
      scenario: {
        id: scenario.id,
        number: scenario.num,
        label: scenario.label,
        title: scenario.title,
        mode: scenario.mode
      },
      vhost: STARTER_VHOST,
      summary: {
        exchanges: target.exchanges.length,
        queues: target.queues.length,
        bindings: target.bindings.length,
        producers: target.producers.length,
        consumers: target.consumers.length
      },
      exchanges: target.exchanges.map((exchange) => ({
        name: exchange.name,
        type: exchange.type,
        durable: !!exchange.durable,
        auto_delete: !!exchange.autoDelete,
        internal: !!exchange.internal,
        alternate_exchange: exchange.alternateExchange ? ((exchangeById(target, exchange.alternateExchange) || {}).name || "") : "",
        description: exchange.desc || "",
        position: { x: Number(exchange.x) || 0, y: Number(exchange.y) || 0 }
      })),
      queues: target.queues.map((queue) => {
        const counts = starterQueueCounts(queue);
        return {
          name: queue.name,
          durable: !!queue.durable,
          auto_delete: !!queue.autoDelete,
          dead_letter_queue: starterIsDlq(target, queue),
          arguments: queueArguments(queue),
          description: queue.desc || "",
          position: { x: Number(queue.x) || 0, y: Number(queue.y) || 0 },
          runtime: {
            ready: counts.ready,
            unacked: counts.unacked,
            received: Number(queue.received) || 0,
            nacks: Number(queue.nacks) || 0,
            dropped: Number(queue.dropped) || 0,
            expired: Number(queue.expired) || 0
          }
        };
      }),
      bindings: target.bindings.map((binding) => {
        const exchange = exchangeById(target, binding.exchange);
        const queue = queueById(target, binding.queue);
        return {
          source: exchange ? exchange.name : binding.exchange,
          destination: queue ? queue.name : binding.queue,
          destination_type: "queue",
          routing_key: binding.key || "",
          arguments: binding.args || {},
          semantics: starterBindingSemantics(binding, exchange)
        };
      }),
      producers: target.producers.map((producer) => {
        const exchange = exchangeById(target, producer.exchangeId);
        return {
          name: producer.name,
          exchange: exchange ? exchange.name : "",
          routing_key: producer.defaultKey || "",
          description: producer.desc || "",
          runtime: { published: Number(producer.published) || 0 }
        };
      }),
      consumers: target.consumers.map((consumer) => {
        const queue = queueById(target, consumer.queueId);
        return {
          name: consumer.name,
          queue: queue ? queue.name : "",
          active: !!consumer.active,
          auto_ack: !!consumer.autoAck,
          prefetch: Math.max(1, Number(consumer.prefetch) || 1),
          description: consumer.desc || "",
          runtime: {
            delivered: Number(consumer.delivered) || 0,
            acked: Number(consumer.acked) || 0
          }
        };
      }),
      publication: {
        source: (target.pub && target.pub.source) || "",
        exchange: target.pub && target.pub.source ? ((target.producers.find((producer) => producer.id === target.pub.source) || {}).name || "") : "",
        routing_key: (target.pub && target.pub.key) || "",
        headers: starterParseHeaders(target.pub && target.pub.headers),
        payload: String((target.pub && target.pub.payload) || ""),
        count: Number((target.pub && target.pub.count) || 1)
      },
      notes: [
        "Сообщения в очередях не включены в topology.json; полный снимок находится в project/rabbit-lab-project.json.",
        "Consumers, prefetch, ack/nack и publisher confirms не являются частью RabbitMQ definitions.",
        "Позиции узлов сохранены для воспроизведения схемы в редакторе."
      ]
    };
  }

  function buildTopologyMermaid() {
    const target = state();
    const lines = ["%% rabbit.lab topology", "flowchart LR"];
    const producerIds = {};
    const exchangeIds = {};
    const queueIds = {};
    const consumerIds = {};

    target.producers.forEach((producer, index) => {
      const id = "p" + index;
      producerIds[producer.id] = id;
      lines.push("  " + id + "[\"producer · " + starterMermaidLabel(producer.name) + "\"]");
    });
    target.exchanges.forEach((exchange, index) => {
      const id = "x" + index;
      exchangeIds[exchange.id] = id;
      lines.push("  " + id + "[\"exchange · " + starterMermaidLabel(exchange.name) + "<br/>" + starterMermaidLabel(exchange.type) + "\"]");
    });
    target.queues.forEach((queue, index) => {
      const id = "q" + index;
      queueIds[queue.id] = id;
      const suffix = starterIsDlq(target, queue) ? " · DLQ" : "";
      lines.push("  " + id + "[\"queue · " + starterMermaidLabel(queue.name) + suffix + "\"]");
    });
    target.consumers.forEach((consumer, index) => {
      const id = "c" + index;
      consumerIds[consumer.id] = id;
      const slot = consumer.active ? "" : " · paused";
      lines.push("  " + id + "([\"consumer · " + starterMermaidLabel(consumer.name) + slot + "\"])");
    });

    let edges = 0;
    target.producers.forEach((producer) => {
      const source = producerIds[producer.id];
      const exchange = exchangeById(target, producer.exchangeId);
      const destination = exchange && exchangeIds[exchange.id];
      if (!source || !destination) return;
      const label = producer.defaultKey || "routing key";
      lines.push("  " + source + " -->|\"" + starterMermaidLabel(label) + "\"| " + destination);
      edges += 1;
    });
    target.bindings.forEach((binding) => {
      const exchange = exchangeById(target, binding.exchange);
      const queue = queueById(target, binding.queue);
      const source = exchange && exchangeIds[exchange.id];
      const destination = queue && queueIds[queue.id];
      if (!source || !destination) return;
      let label = binding.key || (exchange.type === "fanout" ? "fanout" : "binding");
      if (exchange.type === "headers") {
        label = "headers " + ((binding.args && binding.args["x-match"]) || "all");
      }
      lines.push("  " + source + " -->|\"" + starterMermaidLabel(label) + "\"| " + destination);
      edges += 1;
    });
    target.consumers.forEach((consumer) => {
      const queue = queueById(target, consumer.queueId);
      const source = queue && queueIds[queue.id];
      const destination = consumerIds[consumer.id];
      if (!source || !destination) return;
      lines.push("  " + source + " --> " + destination);
      edges += 1;
    });

    if (!edges) lines.push("  %% связей нет: добавьте producer/exchange/queue/binding в rabbit.lab");
    return lines.join("\n") + "\n";
  }

  function starterMermaidLabel(value) {
    return String(value == null ? "" : value)
      .replace(/\\/g, "/")
      .replace(/"/g, "'")
      .replace(/\r?\n/g, " ")
      .replace(/[\[\]{}|]/g, " ")
      .trim() || "—";
  }

  function buildTopologyMarkdown(context) {
    const ctx = context || starterContext();
    const target = ctx.target;
    const scenario = ctx.scenario;
    const lines = [];
    lines.push("# rabbit.lab · " + scenario.title);
    lines.push("");
    lines.push("> Учебная топология, сгенерированная rabbit.lab " + starterDate(ctx.generatedAt) + ".");
    lines.push("> Это спецификация модели, а не production-аудит. Перед запуском в проекте проверьте");
    lines.push("> credentials, TLS, права, лимиты и схему резервного копирования.");
    lines.push("");
    lines.push("Сценарий: " + starterCode(scenario.num + " · " + scenario.label) + ". Vhost: " + starterCode(STARTER_VHOST) + ".");
    lines.push("");
    lines.push("## Кратко");
    lines.push("");
    lines.push("| Exchanges | Queues | Bindings | Producers | Consumers |");
    lines.push("|---:|---:|---:|---:|---:|");
    lines.push("| " + target.exchanges.length + " | " + target.queues.length + " | " + target.bindings.length + " | " + target.producers.length + " | " + target.consumers.length + " |");
    lines.push("");
    lines.push("## Exchanges");
    lines.push("");
    lines.push("| Exchange | Тип | durable | auto-delete | internal | alternate | Описание |");
    lines.push("|---|---|---|---|---|---|---|");
    target.exchanges.forEach((exchange) => {
      const alternate = exchange.alternateExchange ? ((exchangeById(target, exchange.alternateExchange) || {}).name || "") : "";
      lines.push("| " + starterCode(exchange.name) + " | " + starterCode(exchange.type) + " | " + starterBool(exchange.durable) + " | " + starterBool(exchange.autoDelete) + " | " + starterBool(exchange.internal) + " | " + (alternate ? starterCode(alternate) : "—") + " | " + markdownCell(exchange.desc || "—") + " |");
    });
    if (!target.exchanges.length) lines.push("| — | — | — | — | — | — | Нет exchanges |");
    lines.push("");
    lines.push("## Queues");
    lines.push("");
    lines.push("| Очередь | Роль | durable | auto-delete | Аргументы | Описание |");
    lines.push("|---|---|---|---|---|---|");
    target.queues.forEach((queue) => {
      lines.push("| " + starterCode(queue.name) + " | " + (starterIsDlq(target, queue) ? "DLQ" : "queue") + " | " + starterBool(queue.durable) + " | " + starterBool(queue.autoDelete) + " | " + (Object.keys(queueArguments(queue)).length ? starterArgs(queueArguments(queue)) : "—") + " | " + markdownCell(queue.desc || "—") + " |");
    });
    if (!target.queues.length) lines.push("| — | — | — | — | — | Нет queues |");
    lines.push("");
    lines.push("### Снимок очередей");
    lines.push("");
    lines.push("| Очередь | Ready | Unacked | Received | Nacks | Dropped | Expired |");
    lines.push("|---|---:|---:|---:|---:|---:|---:|");
    target.queues.forEach((queue) => {
      const counts = starterQueueCounts(queue);
      lines.push("| " + starterCode(queue.name) + " | " + counts.ready + " | " + counts.unacked + " | " + (Number(queue.received) || 0) + " | " + (Number(queue.nacks) || 0) + " | " + (Number(queue.dropped) || 0) + " | " + (Number(queue.expired) || 0) + " |");
    });
    if (!target.queues.length) lines.push("| — | 0 | 0 | 0 | 0 | 0 | 0 |");
    lines.push("");
    lines.push("## Bindings");
    lines.push("");
    lines.push("| Exchange | Очередь | Routing key | Аргументы | Семантика |");
    lines.push("|---|---|---|---|---|");
    target.bindings.forEach((binding) => {
      const exchange = exchangeById(target, binding.exchange);
      const queue = queueById(target, binding.queue);
      lines.push("| " + starterCode(exchange ? exchange.name : binding.exchange) + " | " + starterCode(queue ? queue.name : binding.queue) + " | " + starterCode(binding.key || "—") + " | " + (Object.keys(binding.args || {}).length ? starterArgs(binding.args) : "—") + " | " + starterBindingSemantics(binding, exchange) + " |");
    });
    if (!target.bindings.length) lines.push("| — | — | — | — | Нет bindings |");
    lines.push("");
    lines.push("## Producers");
    lines.push("");
    lines.push("| Producer | Exchange | Ключ по умолчанию | Опубликовано | Описание |");
    lines.push("|---|---|---|---:|---|");
    target.producers.forEach((producer) => {
      const exchange = exchangeById(target, producer.exchangeId);
      lines.push("| " + starterCode(producer.name) + " | " + starterCode(exchange ? exchange.name : "—") + " | " + starterCode(producer.defaultKey || "—") + " | " + (Number(producer.published) || 0) + " | " + markdownCell(producer.desc || "—") + " |");
    });
    if (!target.producers.length) lines.push("| — | — | — | 0 | Нет producers |");
    lines.push("");
    lines.push("## Consumers");
    lines.push("");
    lines.push("| Consumer | Очередь | Состояние | Ack | Prefetch | Delivered | Acked | Описание |");
    lines.push("|---|---|---|---|---:|---:|---:|---|");
    target.consumers.forEach((consumer) => {
      const queue = queueById(target, consumer.queueId);
      lines.push("| " + starterCode(consumer.name) + " | " + starterCode(queue ? queue.name : "—") + " | " + starterConsumerState(consumer) + " | " + (consumer.autoAck ? "auto-ack" : "manual") + " | " + (Number(consumer.prefetch) || 0) + " | " + (Number(consumer.delivered) || 0) + " | " + (Number(consumer.acked) || 0) + " | " + markdownCell(consumer.desc || "—") + " |");
    });
    if (!target.consumers.length) lines.push("| — | — | — | — | — | 0 | 0 | Нет consumers |");
    lines.push("");
    lines.push("## Аргументы очередей");
    lines.push("");
    if (starterQueueArgRows(target).length) {
      lines.push("| Аргумент | Значение | Очереди | Что делает |");
      lines.push("|---|---|---|---|");
      starterQueueArgRows(target).forEach((row) => {
        lines.push("| " + starterCode(row.key) + " | " + starterCode(String(row.value)) + " | " + row.queues.map(starterCode).join(", ") + " | " + (STARTER_ARG_MEANINGS[row.key] || "Специфичный для RabbitMQ аргумент.") + " |");
      });
    } else {
      lines.push("В этой топологии нет дополнительных аргументов очередей. Очереди используют defaults RabbitMQ.");
    }
    lines.push("");
    lines.push("`x-overflow=drop-head` является значением по умолчанию и поэтому может не попадать в definitions.json. Это нормально.");
    lines.push("");
    lines.push("## Пример публикации");
    lines.push("");
    lines.push("- Producer: " + starterCode(ctx.source));
    lines.push("- Exchange: " + starterCode(ctx.exchangeName || "default"));
    lines.push("- Routing key: " + starterCode(ctx.routingKey || "—"));
    lines.push("- Headers: " + starterCode(starterHeaders(ctx)));
    lines.push("- Count: " + Number((target.pub && target.pub.count) || 1));
    lines.push("");
    lines.push(starterFence(ctx.prettyPayload || "{}", "json"));
    lines.push("");
    lines.push("## Диаграмма");
    lines.push("");
    lines.push(starterFence(buildTopologyMermaid(), "mermaid"));
    lines.push("");
    lines.push("## Что переносится в RabbitMQ");
    lines.push("");
    lines.push("- `rabbitmq/definitions.json` содержит exchanges, queues, bindings, vhost и аргументы очередей.");
    lines.push("- Consumers, prefetch, ack/nack и publisher confirms задаются в приложении, а не в definitions.");
    lines.push("- Users, permissions и TLS в definitions намеренно не включены: используйте `.env`, `rabbitmqctl` или secret manager.");
    lines.push("- Позиции узлов и сообщения сохранены только в `project/rabbit-lab-project.json` для повторного импорта в rabbit.lab.");
    lines.push("");
    lines.push("## Файлы для развёртывания");
    lines.push("");
    lines.push("- `README.md` — быстрый старт и полная карта пакета.");
    lines.push("- `docker-compose.yml` — локальный RabbitMQ с management UI.");
    lines.push("- `apps/producer.js` и `apps/consumer.js` — минимальные примеры приложения.");
    lines.push("- `docs/operations.md` — эксплуатация, backup, monitoring и troubleshooting.");
    lines.push("");
    return lines.join("\n") + "\n";
  }

  function buildStarterTree(files) {
    const root = { dirs: new Map(), files: [] };
    Object.keys(files).forEach((path) => {
      const parts = path.split("/");
      let node = root;
      parts.slice(0, -1).forEach((part) => {
        if (!node.dirs.has(part)) node.dirs.set(part, { dirs: new Map(), files: [] });
        node = node.dirs.get(part);
      });
      node.files.push(parts[parts.length - 1]);
    });

    const lines = [STARTER_ROOT + "/"];
    function walk(node, prefix, pathPrefix) {
      const entries = [];
      node.files.slice().sort().forEach((name) => {
        entries.push({ type: "file", name: name, path: pathPrefix + name });
      });
      Array.from(node.dirs.keys()).sort().forEach((name) => {
        entries.push({ type: "dir", name: name, node: node.dirs.get(name), path: pathPrefix + name + "/" });
      });
      entries.forEach((entry, index) => {
        const last = index === entries.length - 1;
        const branch = last ? "└─ " : "├─ ";
        const nextPrefix = prefix + (last ? "   " : "│  ");
        if (entry.type === "dir") {
          lines.push(prefix + branch + entry.name + "/");
          walk(entry.node, nextPrefix, entry.path);
        } else {
          const meta = STARTER_FILE_META[entry.path];
          lines.push(prefix + branch + entry.name + (meta ? "  — " + meta.note : ""));
        }
      });
    }
    walk(root, "", "");
    return lines.join("\n");
  }

  function starterFormatBytes(bytes) {
    const size = Math.max(0, Number(bytes) || 0);
    if (size >= 1024 * 1024) return (size / 1024 / 1024).toFixed(1).replace(".0", "") + " МБ";
    if (size >= 1024) {
      const kb = size / 1024;
      return (kb >= 10 ? kb.toFixed(0) : kb.toFixed(1)).replace(".0", "") + " КБ";
    }
    return size + " Б";
  }

  function starterCountLabel(count) {
    const value = Math.abs(Number(count) || 0);
    const mod10 = value % 10;
    const mod100 = value % 100;
    let word = "файлов";
    if (mod10 === 1 && mod100 !== 11) word = "файл";
    else if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) word = "файла";
    return value + " " + word;
  }

  function starterTreeSort(left, right) {
    return left.name < right.name ? -1 : left.name > right.name ? 1 : 0;
  }

  function buildStarterTreeData(files) {
    const encoder = new TextEncoder();
    const root = { kind: "dir", name: STARTER_ROOT, path: "", dirs: new Map(), files: [] };
    Object.keys(files).forEach((path) => {
      const parts = path.split("/");
      let node = root;
      parts.slice(0, -1).forEach((part) => {
        if (!node.dirs.has(part)) node.dirs.set(part, { kind: "dir", name: part, path: "", dirs: new Map(), files: [] });
        node = node.dirs.get(part);
      });
      node.files.push({
        name: parts[parts.length - 1],
        path: path,
        bytes: encoder.encode(String(files[path])).length
      });
    });

    function finalize(node, path) {
      node.path = path;
      node.children = [];
      node.bytes = 0;
      node.count = 0;
      node.files.slice().sort(starterTreeSort).forEach((file) => {
        node.children.push({ kind: "file", name: file.name, path: file.path, bytes: file.bytes });
        node.bytes += file.bytes;
        node.count += 1;
      });
      Array.from(node.dirs.keys()).sort().forEach((name) => {
        const child = node.dirs.get(name);
        finalize(child, path ? path + name + "/" : name + "/");
        node.children.push(child);
        node.bytes += child.bytes;
        node.count += child.count;
      });
      node.dirs.clear();
      return node;
    }

    return finalize(root, "");
  }

  function buildStarterBrowserRows(files) {
    const root = buildStarterTreeData(files);
    const rows = [];
    function walk(node, depth) {
      rows.push({
        kind: node.kind,
        name: node.name,
        path: node.path,
        bytes: node.bytes,
        count: node.count,
        depth: depth
      });
      if (node.kind === "dir") node.children.forEach((child) => walk(child, depth + 1));
    }
    walk(root, 0);
    return { root: root, rows: rows };
  }

  function starterFindNode(node, path) {
    if (node.path === path) return node;
    if (node.kind !== "dir") return null;
    for (let index = 0; index < node.children.length; index += 1) {
      const found = starterFindNode(node.children[index], path);
      if (found) return found;
    }
    return null;
  }

  function starterNodeNote(node) {
    if (node.kind === "dir") {
      const meta = STARTER_FOLDER_META[node.path] || {};
      return (meta.note || "раздел проекта") + " · " + starterCountLabel(node.count);
    }
    const meta = STARTER_FILE_META[node.path] || {};
    return meta.note || "файл пакета";
  }

  function renderStarterBrowser(files) {
    const model = buildStarterBrowserRows(files);
    return model.rows.map((node) => {
      const dir = node.kind === "dir";
      const name = node.name + (dir ? "/" : "");
      const note = starterNodeNote(node);
      const size = starterFormatBytes(node.bytes);
      const detailLabel = dir ? "Открыть описание раздела " + name : "Открыть описание файла " + name;
      const downloadLabel = dir ? "Скачать папку " + name + " как ZIP" : "Скачать файл " + name;
      return '<div class="sim-export-row sim-export-row--' + (dir ? "dir" : "file") + '" style="--depth:' + node.depth + '" role="listitem">' +
        '<button class="sim-export-node" type="button" data-export-detail="' + esc(node.path) + '" data-detail-kind="' + (dir ? "dir" : "file") + '" title="' + esc(detailLabel) + '">' +
          '<span class="sim-export-node-icon" aria-hidden="true">' + (dir ? "▸" : "·") + '</span>' +
          '<span class="sim-export-node-copy"><span class="sim-export-node-name">' + esc(name) + '</span><span class="sim-export-node-note">' + esc(note) + '</span></span>' +
          '<span class="sim-export-node-size">' + esc(size) + '</span>' +
        '</button>' +
        '<button class="sim-export-download" type="button" data-export-' + (dir ? "folder" : "part") + '="' + esc(node.path) + '" aria-label="' + esc(downloadLabel) + '" title="' + esc(downloadLabel) + '">' + (dir ? "ZIP" : "↓") + '</button>' +
      '</div>';
    }).join("");
  }

  function starterTopologyFacts(ctx, path) {
    const target = ctx.target;
    const facts = [];
    facts.push(["Сценарий", ctx.scenario.num + " · " + ctx.scenario.title]);
    facts.push(["Exchanges", target.exchanges.length
      ? target.exchanges.map((exchange) => exchange.name + " (" + exchange.type + ")").join(", ")
      : "—"]);
    facts.push(["Очереди", target.queues.length ? target.queues.map((queue) => queue.name).join(", ") : "—"]);
    facts.push(["Bindings", String(target.bindings.length)]);
    const clientLayer = !path || path === "README.md" || path.indexOf("apps/") === 0 || path.indexOf("docs/") === 0;
    if (clientLayer) {
      facts.push(["Producer", ctx.producerName + (ctx.routingKey ? " → " + ctx.routingKey : "")]);
      facts.push(["Consumer", ctx.consumerName + " · prefetch " + ctx.prefetch + (ctx.autoAck ? " · auto-ack" : " · manual ack")]);
    }
    if (path === "docker-compose.yml" || path.indexOf("rabbitmq/") === 0) {
      facts.push(["Порты", "AMQP 5672 · management 15672"]);
      facts.push(["Vhost", STARTER_VHOST]);
    }
    if (path.indexOf("scripts/") === 0) {
      facts.push(["Проверка", "management API · routed=true"]);
      facts.push(["Exchange", ctx.exchangeName || "—"]);
      facts.push(["Очередь", ctx.queueName || "—"]);
    }
    return facts;
  }

  function starterDetailSection(title, body) {
    if (!body) return "";
    return '<section class="sim-export-detail-section"><h3>' + esc(title) + "</h3>" + body + "</section>";
  }

  function starterDetailParagraphs(items) {
    return (items || []).filter(Boolean).map((text) => "<p>" + esc(text) + "</p>").join("");
  }

  function starterDetailList(items) {
    const rows = (items || []).filter(Boolean);
    if (!rows.length) return "";
    return '<ul class="sim-export-detail-list">' +
      rows.map((text) => "<li>" + esc(text) + "</li>").join("") +
      "</ul>";
  }

  function starterDetailFacts(facts) {
    if (!facts || !facts.length) return "";
    return '<dl class="sim-export-detail-facts">' +
      facts.map((row) => '<div><dt>' + esc(row[0]) + "</dt><dd>" + esc(row[1]) + "</dd></div>").join("") +
      "</dl>";
  }

  function starterDetailLinks(paths, files) {
    const rows = (paths || []).filter((path) => Object.prototype.hasOwnProperty.call(files, path));
    if (!rows.length) return "";
    return '<div class="sim-export-detail-links">' +
      rows.map((path) => '<button type="button" data-export-detail="' + esc(path) + '" data-detail-kind="file">' + esc(path) + "</button>").join("") +
      "</div>";
  }

  function starterDetailCommands(commands) {
    if (!commands || !commands.length) return "";
    return '<pre class="sim-export-detail-code">' + esc(commands.join("\n")) + "</pre>";
  }

  function starterDetailChildren(node) {
    if (!node || node.kind !== "dir") return "";
    return '<div class="sim-export-detail-children">' +
      node.children.map((child) => {
        const dir = child.kind === "dir";
        const meta = dir ? (STARTER_FOLDER_META[child.path] || {}) : (STARTER_FILE_META[child.path] || {});
        const note = meta.note || (dir ? "раздел проекта" : "файл пакета");
        return '<button type="button" class="sim-export-detail-child" data-export-detail="' + esc(child.path) + '" data-detail-kind="' + (dir ? "dir" : "file") + '">' +
          '<span class="sim-export-detail-child-copy"><b>' + esc(child.name + (dir ? "/" : "")) + "</b><small>" + esc(note) + "</small></span>" +
          '<span class="sim-export-detail-child-size">' + esc(dir ? starterCountLabel(child.count) : starterFormatBytes(child.bytes)) + "</span>" +
        "</button>";
      }).join("") +
      "</div>";
  }

  function buildStarterFileDetail(path, files) {
    const base = STARTER_FILE_META[path] || {};
    const extra = STARTER_FILE_DETAILS[path] || {};
    const ctx = starterContext();
    const intro = [base.purpose].concat(extra.details || []);
    let html = "";
    html += starterDetailSection("Что это", starterDetailParagraphs(intro));
    html += starterDetailSection("Какие задачи решает", starterDetailList(extra.tasks));
    html += starterDetailSection(
      "Роль в текущей топологии",
      (extra.topologyRole ? '<p class="sim-export-detail-role">' + esc(extra.topologyRole) + "</p>" : "") +
        starterDetailFacts(starterTopologyFacts(ctx, path))
    );
    html += starterDetailSection("Что менять", base.edit ? "<p>" + esc(base.edit) + "</p>" : "");
    html += starterDetailSection("Связанные файлы", starterDetailLinks(extra.related, files));
    html += starterDetailSection("Команды", starterDetailCommands(extra.commands));
    return html;
  }

  function buildStarterFolderDetail(node, files) {
    const meta = STARTER_FOLDER_META[node.path] || {};
    const ctx = starterContext();
    let html = "";
    html += starterDetailSection("Что это", starterDetailParagraphs([meta.purpose].concat(meta.details || [])));
    html += starterDetailSection("Какие задачи решает", starterDetailList(meta.tasks));
    html += starterDetailSection("Что внутри", starterDetailChildren(node));
    html += starterDetailSection(
      "Роль в текущей топологии",
      (meta.topologyRole ? '<p class="sim-export-detail-role">' + esc(meta.topologyRole) + "</p>" : "") +
        starterDetailFacts(starterTopologyFacts(ctx, node.path))
    );
    html += starterDetailSection("Что менять", meta.edit ? "<p>" + esc(meta.edit) + "</p>" : "");
    html += starterDetailSection("Связанные файлы", starterDetailLinks(meta.related, files));
    html += starterDetailSection("Команды", starterDetailCommands(meta.commands));
    return html;
  }

  function openStarterDetail(kind, path) {
    const files = buildStarterKit();
    const tree = buildStarterTreeData(files);
    const node = starterFindNode(tree, path || "");
    if (!node) {
      toast("Элемент структуры не найден: " + (path || "/"), "Экспорт");
      return;
    }
    const modal = document.getElementById("sim-modal-export-detail");
    if (!modal) return;
    const dir = node.kind === "dir";
    const title = document.getElementById("sim-export-detail-title");
    const kindNode = document.getElementById("sim-export-detail-kind");
    const pathNode = document.getElementById("sim-export-detail-path");
    const metaNode = document.getElementById("sim-export-detail-meta");
    const body = document.getElementById("sim-export-detail-body");
    const download = document.getElementById("sim-export-detail-download");
    if (title) title.textContent = dir ? (node.path ? node.name + "/" : STARTER_ROOT + "/") : node.name;
    if (kindNode) kindNode.textContent = dir ? "РЕПОЗИТОРИЙ" : "ФАЙЛ";
    if (pathNode) pathNode.textContent = STARTER_ROOT + "/" + (node.path || "");
    if (metaNode) {
      metaNode.textContent = dir
        ? starterCountLabel(node.count) + " · " + starterFormatBytes(node.bytes) + " · папка"
        : starterFormatBytes(node.bytes) + " · файл";
    }
    if (body) body.innerHTML = dir ? buildStarterFolderDetail(node, files) : buildStarterFileDetail(node.path, files);
    if (download) {
      download.setAttribute("data-kind", node.kind);
      download.setAttribute("data-path", node.path);
      download.textContent = dir ? "Скачать папку ZIP" : "Скачать файл";
    }
    openModal(modal);
    const dialog = modal.querySelector(".sim-dialog");
    if (dialog) dialog.scrollTop = 0;
  }

  function closeStarterDetail() {
    closeModal(document.getElementById("sim-modal-export-detail"));
  }

  function starterMime(path) {
    const ext = String(path).split(".").pop().toLowerCase();
    if (ext === "json") return "application/json;charset=utf-8";
    if (ext === "md") return "text/markdown;charset=utf-8";
    if (ext === "js") return "text/javascript;charset=utf-8";
    if (ext === "yml" || ext === "yaml") return "text/yaml;charset=utf-8";
    return "text/plain;charset=utf-8";
  }

  function starterFolderFiles(files, folderPath) {
    const folder = String(folderPath || "").replace(/\/+$/, "");
    const prefix = folder ? folder + "/" : "";
    const subset = {};
    Object.keys(files).forEach((path) => {
      if (!prefix || path.indexOf(prefix) === 0) subset[path.slice(prefix.length)] = files[path];
    });
    return subset;
  }

  function exportStarterFile(path) {
    try {
      const files = buildStarterKit();
      if (!Object.prototype.hasOwnProperty.call(files, path)) {
        toast("Файл не найден: " + path, "Экспорт");
        return;
      }
      const blob = new Blob([files[path]], { type: starterMime(path) });
      downloadBlob(path.split("/").pop(), blob);
      addLog("sys", "export · file · " + path + " · " + blob.size + " bytes");
      renderLog();
      persistSoon();
      toast("Файл скачан: " + path, "Экспорт");
    } catch (error) {
      toast("Не удалось скачать файл: " + error.message, "Ошибка экспорта");
    }
  }

  function exportStarterFolder(path) {
    try {
      const files = buildStarterKit();
      const folder = String(path || "").replace(/\/+$/, "");
      const subset = starterFolderFiles(files, folder);
      const count = Object.keys(subset).length;
      if (!count) {
        toast("В выбранной папке нет файлов.", "Экспорт");
        return;
      }
      const rootName = folder ? STARTER_ROOT + "/" + folder : STARTER_ROOT;
      const blob = buildStarterZipBlob(subset, rootName);
      const name = folder
        ? STARTER_ROOT + "-" + folder.replace(/\//g, "-") + ".zip"
        : starterZipName();
      downloadBlob(name, blob);
      addLog("sys", "export · folder · " + (folder || "/") + " · " + count + " files · " + blob.size + " bytes");
      renderLog();
      persistSoon();
      toast("Папка скачана: " + (folder || STARTER_ROOT) + " · " + count + " файлов.", "Экспорт");
    } catch (error) {
      toast("Не удалось собрать папку: " + error.message, "Ошибка экспорта");
    }
  }

  function downloadStarterSelection(kind, path) {
    if (kind === "dir") exportStarterFolder(path || "");
    else exportStarterFile(path);
  }

  function buildStarterReadme(ctx, files) {
    const target = ctx.target;
    const scenario = ctx.scenario;
    const tree = buildStarterTree(files);
    const queueNames = target.queues.map((queue) => starterCode(queue.name)).join(", ") || "—";
    const exchangeNames = target.exchanges.map((exchange) => starterCode(exchange.name)).join(", ") || "—";
    const lines = [];
    lines.push("# rabbit.lab starter kit");
    lines.push("");
    lines.push("**Сценарий:** " + scenario.num + " · " + scenario.title + "  ");
    lines.push("**Сгенерировано:** " + starterDate(ctx.generatedAt) + "  ");
    lines.push("**Vhost:** " + starterCode(STARTER_VHOST) + "  ");
    lines.push("**Брокер:** RabbitMQ " + STARTER_RABBIT_VERSION + " (management image)");
    lines.push("");
    lines.push("> Это учебный стартовый пакет, а не готовый production-кластер. Перед использованием");
    lines.push("> в реальном проекте смените dev-пароль, включите TLS, настройте права, лимиты,");
    lines.push("> мониторинг и резервное копирование. Симулятор не заменяет нагрузочное тестирование.");
    lines.push("");
    lines.push("## Что внутри");
    lines.push("");
    lines.push("Один архив содержит всё, что нужно для локального запуска описанного брокера и передачи");
    lines.push("схемы разработчику: Docker Compose с init-импортом definitions, конфиг RabbitMQ, документацию,");
    lines.push("Mermaid-схему, скрипты импорта и проверки, а также примеры producer/consumer на Node.js.");
    lines.push("");
    lines.push("**Топология:** exchanges — " + exchangeNames + "; queues — " + queueNames + ".");
    lines.push("Полные таблицы и маршруты — в `topology/topology.md`.");
    lines.push("");
    lines.push("## Состав пакета");
    lines.push("");
    lines.push(starterFence(tree, "text"));
    lines.push("");
    lines.push("### Файлы и зоны ответственности");
    lines.push("");
    lines.push("| Файл | Назначение | Что делать |");
    lines.push("|---|---|---|");
    Object.keys(files).sort().forEach((path) => {
      const meta = STARTER_FILE_META[path];
      lines.push("| " + starterCode(path) + " | " + (meta ? meta.purpose : "Файл пакета.") + " | " + (meta ? meta.edit : "—") + " |");
    });
    lines.push("");
    lines.push("`definitions.json` не поддерживает комментарии по стандарту JSON. Поэтому назначение");
    lines.push("каждого поля описано здесь и в `docs/architecture.md`, а не внутри JSON.");
    lines.push("");
    lines.push("## 1. Быстрый старт");
    lines.push("");
    lines.push("### Требования");
    lines.push("");
    lines.push("- Docker Engine или Docker Desktop с Compose v2.");
    lines.push("- Свободные порты `5672` (AMQP) и `15672` (management UI).");
    lines.push("- Для примеров приложения — Node.js 20+ и npm.");
    lines.push("");
    lines.push("### Запуск");
    lines.push("");
    lines.push("Скопируйте `.env.example` в `.env` и при необходимости измените значения.");
    lines.push("");
    lines.push("**Windows PowerShell:**");
    lines.push(starterFence("Copy-Item .env.example .env\nnotepad .env\ndocker compose up -d\ndocker compose ps -a", "powershell"));
    lines.push("");
    lines.push("**macOS / Linux:**");
    lines.push(starterFence("cp .env.example .env\n# откройте .env и смените пароль\ndocker compose up -d\ndocker compose ps -a", "bash"));
    lines.push("");
    lines.push("Management UI: http://localhost:15672  ");
    lines.push("Логин и пароль по умолчанию из `.env.example`: `rabbitlab` / `rabbitlab-dev`.");
    lines.push("Эти значения только для локальной разработки; смените их до любого общего окружения.");
    lines.push("");
    lines.push("Проверить, что брокер здоров, а топология импортирована:");
    lines.push(starterFence("docker compose ps -a\ndocker compose logs --tail=80 rabbitmq\ndocker compose logs --tail=40 rabbitmq-init", "bash"));
    lines.push("");
    lines.push("Сервис `rabbitmq-init` одноразовый: после успешного импорта он завершается с кодом `0`.");
    lines.push("Это нормально; в `docker compose ps` без `-a` его уже не видно.");
    lines.push("");
    lines.push("## 2. Как загружается топология");
    lines.push("");
    lines.push("На пустом volume RabbitMQ создаёт пользователя и vhost `/` из переменных окружения.");
    lines.push("После healthcheck Compose запускает одноразовый `rabbitmq-init`: он вызывает");
    lines.push("`rabbitmqadmin import /definitions.json` через management API и повторяет попытку,");
    lines.push("пока API не будет готов. Топология появляется после импорта, а не при старте узла.");
    lines.push("");
    lines.push("Почему не `management.load_definitions`: если RabbitMQ загружает definitions при старте,");
    lines.push("он пропускает создание `DEFAULT_USER` и `DEFAULT_VHOST`. Тогда credentials из `.env`");
    lines.push("не работают, а management API отвечает `401`. Поэтому импорт вынесен в init-сервис.");
    lines.push("");
    lines.push("Важно:");
    lines.push("");
    lines.push("- `rabbitmq-init` — одноразовый контейнер. Код `0` означает успешный импорт, `1` — ошибку.");
    lines.push("- Если definitions изменились, примените их скриптом; перезапуск RabbitMQ сам их не перечитает.");
    lines.push("- Импорт **добавляет и обновляет** сущности, но не удаляет то, чего нет в новом файле.");
    lines.push("- Consumers, prefetch, ack/nack и publisher confirms в definitions не входят — это поведение приложения.");
    lines.push("- `definitions.json` не содержит users и паролей: пользователь создаётся из `.env` на пустом volume.");
    lines.push("- Смена `.env` на существующем volume не меняет пароль. Используйте `rabbitmqctl` или `docker compose down -v` для локального сброса.");
    lines.push("");
    lines.push("Повторный импорт вручную:");
    lines.push("");
    lines.push("**Windows PowerShell:**");
    lines.push(starterFence("./scripts/apply-definitions.ps1", "powershell"));
    lines.push("**macOS / Linux:**");
    lines.push(starterFence("chmod +x scripts/*.sh\n./scripts/apply-definitions.sh", "bash"));
    lines.push("");
    lines.push("Скрипты используют `RABBITMQ_MANAGEMENT_URL`, `RABBITMQ_USER` и `RABBITMQ_PASS`.");
    lines.push("Bash-версия читает `.env` из корня пакета; PowerShell-версия принимает параметры");
    lines.push("или переменные окружения текущей сессии. Перед запуском из другой папки перейдите");
    lines.push("в корень пакета.");
    lines.push("");
    lines.push("Если `rabbitmq-init` завершился с ошибкой, посмотрите его лог и повторите импорт:");
    lines.push(starterFence("docker compose logs --tail=80 rabbitmq-init\ndocker compose run --rm rabbitmq-init", "bash"));
    lines.push("");
    lines.push("## 3. Проверка после запуска");
    lines.push("");
    lines.push("1. Откройте management UI и убедитесь, что exchanges и queues из таблицы выше видны.");
    lines.push("2. Выполните smoke-test:");
    lines.push("");
    lines.push("**Windows PowerShell:**");
    lines.push(starterFence("./scripts/smoke-test.ps1", "powershell"));
    lines.push("**macOS / Linux:**");
    lines.push(starterFence("./scripts/smoke-test.sh", "bash"));
    lines.push("");
    lines.push("3. Посмотрите очереди через CLI:");
    lines.push(starterFence("docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers", "bash"));
    lines.push("4. Проверьте bindings:");
    lines.push(starterFence("docker compose exec rabbitmq rabbitmqctl list_bindings source_name destination_name routing_key", "bash"));
    lines.push("");
    lines.push("Smoke-test публикует одно тестовое сообщение в первый exchange. Если routing key не");
    lines.push("совпал ни с одним binding, RabbitMQ вернёт `routed: false` — это сигнал проверить");
    lines.push("таблицу bindings в `topology/topology.md`.");
    lines.push("");
    lines.push("## 4. Producer и consumer");
    lines.push("");
    lines.push("Примеры лежат в `apps/`. Они не объявляют топологию заново: ожидают, что definitions");
    lines.push("уже импортированы. Это снижает риск случайно изменить аргументы очереди.");
    lines.push("");
    lines.push(starterFence("cd apps\nnpm install\nnpm run producer", "bash"));
    lines.push("");
    lines.push("В другом терминале:");
    lines.push(starterFence("cd apps\nnpm run consumer", "bash"));
    lines.push("");
    lines.push("Значения по умолчанию в примерах:");
    lines.push("");
    lines.push("- exchange: `" + ctx.exchangeName + "`");
    lines.push("- routing key: `" + ctx.routingKey + "`");
    lines.push("- queue: `" + ctx.queueName + "`");
    lines.push("- prefetch: `" + ctx.prefetch + "`");
    lines.push("");
    lines.push("Переопределить их можно переменными `EXCHANGE`, `ROUTING_KEY`, `QUEUE`, `PREFETCH` и");
    lines.push("`RABBITMQ_URL`. Полная инструкция — в `apps/README.md`.");
    lines.push("");
    lines.push("## 5. Как менять топологию");
    lines.push("");
    lines.push("Рекомендуемый цикл:");
    lines.push("");
    lines.push("1. Меняйте схему в rabbit.lab и проверяйте routing в симуляторе.");
    lines.push("2. Экспортируйте starter kit заново.");
    lines.push("3. Сравните новый `rabbitmq/definitions.json` с текущим (diff/ревью).");
    lines.push("4. Примените definitions в dev и прогоните smoke-test.");
    lines.push("5. Только после этого передавайте изменения в общее окружение.");
    lines.push("");
    lines.push("Не редактируйте definitions в UI «на лету» как единственный источник правды:");
    lines.push("изменения потеряются при следующем экспорте из конструктора.");
    lines.push("");
    lines.push("## 6. Безопасность перед общим окружением");
    lines.push("");
    lines.push("- Смените `RABBITMQ_PASS`; не используйте `rabbitlab-dev` вне локальной машины.");
    lines.push("- Не коммитьте `.env`. Добавьте его в `.gitignore` своего проекта.");
    lines.push("- Закройте management UI от публичного интернета или поставьте за VPN/reverse proxy с TLS.");
    lines.push("- Создайте отдельного пользователя приложения с минимальными permissions на vhost.");
    lines.push("- Включите TLS для AMQP 5671 и management 15671 в общем окружении.");
    lines.push("- Настройте лимиты памяти/диска и алерты на `memory`/`disk_free` alarms.");
    lines.push("- Делайте backup definitions и volume; проверяйте restore, а не только создание копии.");
    lines.push("");
    lines.push("Подробный runbook: `docs/operations.md`.");
    lines.push("");
    lines.push("## 7. Ограничения учебной модели");
    lines.push("");
    lines.push("- Симулятор показывает routing, TTL, DLX, retry и consumers, но не эмулирует flow control, кластер, quorum queues, federation, shovel и плагины.");
    lines.push("- `topology/topology.json` и `project/rabbit-lab-project.json` — снимки модели, а не экспорт метрик production-брокера.");
    lines.push("- Сообщения в project-файле могут содержать тестовые данные. Проверьте их перед публикацией.");
    lines.push("- Примеры приложения намеренно минимальны: в них нет retry-политики, схемы валидации, трассировки и graceful degradation.");
    lines.push("");
    lines.push("## 8. Если что-то не работает");
    lines.push("");
    lines.push("| Симптом | Что проверить |");
    lines.push("|---|---|");
    lines.push("| UI не открывается | `docker compose ps -a`, порт 15672, логи rabbitmq, образ с тегом `-management` |");
    lines.push("| Очередей нет | `docker compose logs rabbitmq-init`, код выхода init, ошибки 401/импорта |");
    lines.push("| `rabbitmq-init` exited 1 | `docker compose logs rabbitmq-init`; повторите `docker compose run --rm rabbitmq-init` |");
    lines.push("| Не пускает в UI | Пароль из `.env` действует только при первом создании volume; см. `docs/operations.md` |");
    lines.push("| Сообщение не доходит | `routing key`, тип exchange, bindings, headers и `x-match` |");
    lines.push("| Consumer ничего не получает | Очередь пуста, consumer подключён к другой очереди, prefetch/ack |");
    lines.push("| После смены `.env` старый пароль | Пересоздайте volume или смените пользователя через `rabbitmqctl` |");
    lines.push("");
    lines.push("Полный troubleshooting — в `docs/operations.md`.");
    lines.push("");
    lines.push("## 9. О пакете");
    lines.push("");
    lines.push("Пакет сгенерирован образовательным проектом rabbit.lab. RabbitMQ — товарный знак");
    lines.push("правообладателя; проект не связан с Broadcom, VMware или командой RabbitMQ.");
    lines.push("Перед production-запуском необходимы независимые security, load и disaster-recovery проверки.");
    lines.push("");
    return lines.join("\n") + "\n";
  }

  function buildArchitectureDoc(ctx) {
    const target = ctx.target;
    const scenario = ctx.scenario;
    const lines = [];
    lines.push("# Архитектура и соответствие RabbitMQ");
    lines.push("");
    lines.push("**Сценарий:** " + scenario.num + " · " + scenario.title + "  ");
    lines.push("**Vhost:** " + starterCode(STARTER_VHOST) + "  ");
    lines.push("**Файл топологии:** " + starterCode("rabbitmq/definitions.json"));
    lines.push("");
    lines.push("## Как читать пакет");
    lines.push("");
    lines.push("- `rabbitmq/definitions.json` — декларативная топология RabbitMQ. Это главный артефакт для развёртывания.");
    lines.push("- `docker-compose.yml` — локальная обвязка: образ, порты, volume, healthcheck и init-импорт definitions.");
    lines.push("- `topology/` — человекочитаемое описание и Mermaid-схема; не влияет на брокер.");
    lines.push("- `apps/` — примеры клиентского кода; не являются частью definitions.");
    lines.push("- `project/` — снимок rabbit.lab для повторного открытия в конструкторе.");
    lines.push("");
    lines.push("## Импорт definitions и пользователь");
    lines.push("");
    lines.push("На пустом volume RabbitMQ создаёт пользователя и vhost `/` из переменных окружения.");
    lines.push("После healthcheck сервис `rabbitmq-init` импортирует `rabbitmq/definitions.json` через");
    lines.push("management API с помощью `rabbitmqadmin`; при необходимости он повторяет попытки.");
    lines.push("");
    lines.push("`management.load_definitions` намеренно не включён. Если RabbitMQ загружает definitions");
    lines.push("при старте, он пропускает создание `DEFAULT_USER`/`DEFAULT_VHOST`, и credentials из");
    lines.push("`.env` перестают работать. Поэтому импорт вынесен в отдельный одноразовый контейнер.");
    lines.push("");
    lines.push("`rabbitmq-init` с кодом `0` — успех; с кодом `1` — ошибка. Повторный импорт:");
    lines.push("");
    lines.push(starterFence("docker compose logs --tail=80 rabbitmq-init\ndocker compose run --rm rabbitmq-init", "bash"));
    lines.push("");
    lines.push("## Соответствие модели и RabbitMQ");
    lines.push("");
    lines.push("| Структура rabbit.lab | Куда попадает | Что означает |");
    lines.push("|---|---|---|");
    lines.push("| Exchange | `definitions.exchanges[]` | Точка приёма сообщений: type, durable, auto_delete, internal, аргументы. |");
    lines.push("| Queue | `definitions.queues[]` | Буфер сообщений: durable, auto_delete и аргументы (TTL, DLX, лимиты, приоритеты). |");
    lines.push("| Binding | `definitions.bindings[]` | Правило exchange → queue: routing_key и arguments (включая `x-match` для headers). |");
    lines.push("| Alternate exchange | `exchanges[].arguments[\"alternate-exchange\"]` | Exchange, куда уходит сообщение, если основной не нашёл очередь. |");
    lines.push("| Producer | `apps/producer.js` + конфиг приложения | Клиент, который публикует сообщения. В definitions не хранится. |");
    lines.push("| Consumer | `apps/consumer.js` + конфиг приложения | Клиент, который читает очередь. Prefetch, ack и noAck — на стороне приложения. |");
    lines.push("| Сообщение | Не входит в definitions | В project-файле сохраняется как снимок учебной модели. |");
    lines.push("| TTL / DLX / retry | Аргументы queue + bindings | Настраиваются в RabbitMQ декларативно, обработка повторов — приложением. |");
    lines.push("");
    lines.push("## Маршрутизация в этой схеме");
    lines.push("");
    if (target.bindings.length) {
      target.bindings.forEach((binding) => {
        const exchange = exchangeById(target, binding.exchange);
        const queue = queueById(target, binding.queue);
        lines.push("- " + starterCode(exchange ? exchange.name : binding.exchange) + " → " + starterCode(queue ? queue.name : binding.queue) + ", ключ " + starterCode(binding.key || "—") + ": " + starterBindingSemantics(binding, exchange) + ".");
      });
    } else {
      lines.push("- Bindings отсутствуют. Сообщения не будут маршрутизироваться, пока не добавлена хотя бы одна связь.");
    }
    lines.push("");
    lines.push("Правила типов exchange:");
    lines.push("");
    lines.push("- **direct** — точное совпадение routing key.");
    lines.push("- **fanout** — копия во все связанные очереди, routing key игнорируется.");
    lines.push("- **topic** — маска `*` заменяет одно слово, `#` — ноль или больше слов, слова разделены точкой.");
    lines.push("- **headers** — сравнение заголовков; `x-match=all` требует все пары, `x-match=any` — хотя бы одну.");
    lines.push("");
    lines.push("## Аргументы очередей");
    lines.push("");
    const argRows = starterQueueArgRows(target);
    if (argRows.length) {
      lines.push("| Аргумент | Значение в модели | Очереди | Смысл |");
      lines.push("|---|---|---|---|");
      argRows.forEach((row) => {
        lines.push("| " + starterCode(row.key) + " | " + starterCode(String(row.value)) + " | " + row.queues.map(starterCode).join(", ") + " | " + (STARTER_ARG_MEANINGS[row.key] || "Аргумент RabbitMQ.") + " |");
      });
    } else {
      lines.push("Дополнительных аргументов нет: используются defaults RabbitMQ.");
    }
    lines.push("");
    lines.push("TTL и DLX — это аргументы очереди. Они влияют на уже опубликованные сообщения только");
    lines.push("после применения definitions к брокеру. В симуляторе они проверяются виртуально.");
    lines.push("");
    lines.push("## Доставка, ack и повторная обработка");
    lines.push("");
    lines.push("- Consumer получает сообщение и подтверждает его через `ack` только после успешной обработки.");
    lines.push("- `nack` с `requeue=true` возвращает сообщение в очередь и может привести к бесконечному циклу.");
    lines.push("- `nack` с `requeue=false` отправляет сообщение в DLX, если он настроен, иначе сообщение теряется.");
    lines.push("- `prefetch` ограничивает число неподтверждённых сообщений на consumer. Значение по умолчанию в примере: " + ctx.prefetch + ".");
    lines.push("- Publisher confirms подтверждают, что брокер принял сообщение, но не гарантируют обработку consumer.");
    lines.push("- Durable queue хранит сообщения на диске; persistent delivery mode задаёт сохраняемость самих сообщений.");
    lines.push("- Для production обычно нужны quorum queues, политики, ретраи с backoff и отдельный DLQ-процесс.");
    lines.push("");
    lines.push("## Структура definitions.json");
    lines.push("");
    lines.push("| Поле | Назначение |");
    lines.push("|---|---|");
    lines.push("| `rabbit_version` | Версия RabbitMQ, с которой совместим экспорт. Импорт в другой major стоит проверять отдельно. |");
    lines.push("| `vhosts` | Список virtual hosts. В пакете используется `/`. |");
    lines.push("| `parameters` | Компонентные параметры (например, federation/shovel). Здесь пусто. |");
    lines.push("| `global_parameters` | Глобальные настройки узла. Здесь пусто. |");
    lines.push("| `policies` | Политики, применяемые по шаблону имени. Здесь пусто: аргументы заданы на уровне очередей. |");
    lines.push("| `exchanges` | Массив exchanges: name, vhost, type, durable, auto_delete, internal, arguments. |");
    lines.push("| `queues` | Массив очередей: name, vhost, durable, auto_delete, arguments. |");
    lines.push("| `bindings` | Массив связей: source, destination, destination_type, routing_key, arguments. |");
    lines.push("");
    lines.push("В definitions намеренно нет `users` и `permissions`: пароли не должны попадать в");
    lines.push("артефакт, который коммитится в репозиторий. Пользователь создаётся из `.env` при");
    lines.push("первом старте на пустом volume; импорт definitions его не перезаписывает.");
    lines.push("На существующем volume меняйте пароль через `rabbitmqctl`.");
    lines.push("");
    lines.push("## Структура topology.json");
    lines.push("");
    lines.push("- `schema` и `generator` — версия формата и источник.");
    lines.push("- `scenario` — выбранный учебный сценарий.");
    lines.push("- `exchanges`, `queues`, `bindings` — снимок декларативной топологии с позициями и runtime-счётчиками.");
    lines.push("- `producers`, `consumers` — клиентская часть модели; подсказка для приложения.");
    lines.push("- `publication` — пример сообщения, чтобы воспроизвести проверку вручную.");
    lines.push("");
    lines.push("## Ограничения модели");
    lines.push("");
    lines.push("- Нет эмуляции кластера, quorum queues, flow control, federation, shovel и политик.");
    lines.push("- Нет реального TLS, SASL, лимитов и мониторинга.");
    lines.push("- Порядок сообщений, конкуренция consumers и гарантии Exactly-once не моделируются.");
    lines.push("- Один consumer в примере не описывает весь production-пул.");
    lines.push("- Перед запуском в проекте проверьте версию RabbitMQ, тип очереди, DLX-циклы и retry-политику.");
    lines.push("");
    lines.push("## Production-чек-лист");
    lines.push("");
    lines.push("1. Сменить пароли и выдать приложению отдельного пользователя.");
    lines.push("2. Включить TLS и закрыть management UI.");
    lines.push("3. Выбрать classic/quorum queues под требования и настроить политики.");
    lines.push("4. Ограничить memory/disk и настроить алерты.");
    lines.push("5. Настроить backup definitions и volume, проверить restore.");
    lines.push("6. Добавить метрики, трассировку и алерты на глубину очередей и unacked.");
    lines.push("7. Проверить retry/DLX и отсутствие бесконечных циклов.");
    lines.push("8. Зафиксировать версию образа и порядок обновления.");
    lines.push("");
    return lines.join("\n") + "\n";
  }

  function buildOperationsDoc(ctx) {
    const target = ctx.target;
    const queue = ctx.queueName || "queue";
    const exchange = ctx.exchangeName || "exchange";
    const lines = [];
    lines.push("# Эксплуатация RabbitMQ");
    lines.push("");
    lines.push("Runbook для локального стенда и первого переноса в проект. Команды предполагают, что");
    lines.push("вы находитесь в корне пакета. Compose запускает `rabbitmq` и одноразовый `rabbitmq-init`.");
    lines.push("");
    lines.push("## 1. Первый запуск");
    lines.push("");
    lines.push("1. Создайте `.env` из `.env.example` и смените пароль.");
    lines.push("2. Запустите `docker compose up -d`.");
    lines.push("3. Дождитесь статуса `healthy`: `docker compose ps -a`.");
    lines.push("4. Проверьте, что `rabbitmq-init` завершился с кодом `0`: `docker compose ps -a`.");
    lines.push("5. Если код не `0`, посмотрите `docker compose logs rabbitmq-init` и повторите `docker compose run --rm rabbitmq-init`.");
    lines.push("6. Откройте management UI и убедитесь, что exchanges, queues и bindings загружены.");
    lines.push("");
    lines.push("## 2. Ежедневные проверки");
    lines.push("");
    lines.push("- Нет ли `memory`/`disk_free` alarms.");
    lines.push("- Очереди растут не бесконечно: смотрите ready, unacked и consumer count.");
    lines.push("- Consumers живы и prefetch соответствует нагрузке.");
    lines.push("- Нет роста nacks/dropped/expired без ожидаемого DLQ-процесса.");
    lines.push("- Management UI и API доступны только доверенной сети.");
    lines.push("");
    lines.push("Полезные команды:");
    lines.push(starterFence("docker compose exec rabbitmq rabbitmq-diagnostics status\ndocker compose exec rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged consumers\ndocker compose exec rabbitmq rabbitmqctl list_connections name state channels", "bash"));
    lines.push("");
    lines.push("## 3. Пользователи и права");
    lines.push("");
    lines.push("`RABBITMQ_DEFAULT_USER` и `RABBITMQ_DEFAULT_PASS` применяются только при создании");
    lines.push("пустого volume. Смена `.env` на существующем volume не изменит пароль и приведёт");
    lines.push("к ошибке `401` у `rabbitmq-init`; обновите пароль через `rabbitmqctl` или пересоздайте volume.");
    lines.push("");
    lines.push("Создать отдельного пользователя приложения:");
    const permissionExchange = exchange ? exchange.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") : ".*";
    const permissionQueue = queue ? queue.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") : ".*";
    lines.push(starterFence("docker compose exec rabbitmq rabbitmqctl add_user app 'CHANGE_ME'\ndocker compose exec rabbitmq rabbitmqctl set_permissions -p / app \"^" + permissionExchange + "$\" \"^" + permissionExchange + "$\" \"^" + permissionQueue + "$\"", "bash"));
    lines.push("");
    lines.push("Вместо шаблонов выше выдайте минимально необходимые configure/write/read permissions");
    lines.push("на конкретные exchange и queue. Не используйте `.*` для production без необходимости.");
    lines.push("");
    lines.push("Сменить пароль существующего пользователя:");
    lines.push(starterFence("docker compose exec rabbitmq rabbitmqctl change_password rabbitlab 'NEW_PASSWORD'", "bash"));
    lines.push("");
    lines.push("## 4. Изменение топологии");
    lines.push("");
    lines.push("1. Измените схему в rabbit.lab и сделайте новый экспорт.");
    lines.push("2. Сравните `rabbitmq/definitions.json` с текущим файлом.");
    lines.push("3. Примените definitions в dev:");
    lines.push(starterFence("./scripts/apply-definitions.sh", "bash"));
    lines.push("4. Проверьте smoke-test и UI.");
    lines.push("5. Только после проверки переносите файл в общее окружение.");
    lines.push("");
    lines.push("Импорт definitions не удаляет сущности, которых нет в файле. Удаление очереди или");
    lines.push("exchange — отдельная осознанная операция. Сначала убедитесь, что в ней нет нужных");
    lines.push("сообщений и что consumers остановлены.");
    lines.push("");
    lines.push("## 5. Backup и restore");
    lines.push("");
    lines.push("### Экспорт definitions через management API");
    lines.push("");
    lines.push("**Bash:**");
    lines.push(starterFence("curl -fsS -u \"$RABBITMQ_USER:$RABBITMQ_PASS\" \\\n  \"$RABBITMQ_MANAGEMENT_URL/api/definitions\" \\\n  -o backup-definitions-$(date +%F).json", "bash"));
    lines.push("**PowerShell:**");
    lines.push(starterFence("$auth = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(\"$env:RABBITMQ_USER`:$env:RABBITMQ_PASS\"))\nInvoke-WebRequest -Uri \"$env:RABBITMQ_MANAGEMENT_URL/api/definitions\" `\n  -Headers @{ Authorization = \"Basic $auth\" } `\n  -OutFile \"backup-definitions-$(Get-Date -Format yyyy-MM-dd).json\"", "powershell"));
    lines.push("");
    lines.push("Definitions описывает топологию, но не сообщения. Для полного восстановления нужен");
    lines.push("backup volume `/var/lib/rabbitmq` или отдельная стратегия replay из источника.");
    lines.push("Останавливайте брокер перед копированием volume, иначе копия может быть несогласованной.");
    lines.push("");
    lines.push("### Restore");
    lines.push("");
    lines.push("Примените definitions через скрипт или management API. Для восстановления сообщений");
    lines.push("используйте заранее проверенную процедуру восстановления volume/снапшота.");
    lines.push("После restore проверьте consumers, DLQ и права.");
    lines.push("");
    lines.push("## 6. Monitoring");
    lines.push("");
    lines.push("Минимальный набор:");
    lines.push("");
    lines.push("- `messages_ready` и `messages_unacknowledged` по каждой очереди.");
    lines.push("- Число consumers и их стабильность.");
    lines.push("- Publish/deliver/ack rates.");
    lines.push("- `memory`, `disk_free`, file descriptors и alarms.");
    lines.push("- Ошибки каналов и connections.");
    lines.push("");
    lines.push("Management API: `/api/overview`, `/api/queues/%2F`, `/api/nodes`.");
    lines.push("Для production подключите Prometheus-плагин и алерты на рост очередей, отсутствие");
    lines.push("consumers и memory/disk alarms.");
    lines.push("");
    lines.push("## 7. TLS");
    lines.push("");
    lines.push("Для общего окружения не публикуйте 5672 и 15672 напрямую. Пример направления:");
    lines.push("");
    lines.push(starterFence("listeners.ssl.default = 5671\nssl_options.cacertfile = /etc/rabbitmq/certs/ca.pem\nssl_options.certfile = /etc/rabbitmq/certs/server.pem\nssl_options.keyfile = /etc/rabbitmq/certs/server.key\nssl_options.verify = verify_peer\nssl_options.fail_if_no_peer_cert = false\nmanagement.ssl.port = 15671", "ini"));
    lines.push("");
    lines.push("Сертификаты и приватные ключи не должны попадать в этот архив. Используйте secrets");
    lines.push("manager или volume с ограниченными правами. После включения TLS обновите");
    lines.push("`RABBITMQ_URL` на `amqps://...:5671/...`.");
    lines.push("");
    lines.push("## 8. Обновление");
    lines.push("");
    lines.push("1. Сделайте backup definitions и volume.");
    lines.push("2. Прочитайте release notes RabbitMQ и проверьте совместимость definitions.");
    lines.push("3. Зафиксируйте новый тег образа, не используйте плавающий `latest`.");
    lines.push("4. Обновите dev, прогоните smoke-test и проверьте consumers.");
    lines.push("5. Обновляйте production по одному узлу, если используется кластер.");
    lines.push("");
    lines.push("## 9. Troubleshooting");
    lines.push("");
    lines.push("| Симптом | Диагностика | Решение |");
    lines.push("|---|---|---|");
    lines.push("| Контейнер не стартует | `docker compose logs rabbitmq` | Проверьте YAML, права на volumes, занятые порты, память. |");
    lines.push("| UI 404 | Образ без management | Используйте `rabbitmq:3.13-management`, проверьте `enabled_plugins`. |");
    lines.push("| Очередей нет | `docker compose ps -a`, `docker compose logs rabbitmq-init`, код выхода init | Повторите `docker compose run --rm rabbitmq-init` или примените definitions скриптом. |");
    lines.push("| `rabbitmq-init` exited 1 | `docker compose logs rabbitmq-init`: `401` или ошибка импорта | Проверьте credentials/volume; при необходимости пересоздайте volume. |");
    lines.push("| Импорт не удалил сущность | Definitions не удаляет отсутствующие объекты | Удалите сущность через UI/API/CLI осознанно. |");
    lines.push("| Не пускает в UI после смены `.env` | Пароль применён при первом init volume | Смените пароль через `rabbitmqctl` или пересоздайте volume. |");
    lines.push("| Публикация `routed: false` | Нет подходящего binding/key/header | Сверьте `topology/topology.md`, тип exchange и `x-match`. |");
    lines.push("| Consumer не получает сообщения | Не та очередь, нет consumers, prefetch/ack | Проверьте `list_queues`, `list_consumers`, настройки приложения. |");
    lines.push("| Рост unacked | Медленный handler или забытый ack | Уменьшите prefetch, исправьте ack/nack, проверьте таймауты. |");
    lines.push("| Бесконечный retry | `nack requeue=true` без backoff | Ограничьте retries, используйте DLQ/retry queues. |");
    lines.push("| `disk_free` alarm | Мало места | Освободите диск, настройте лимиты и ретеншн. |");
    lines.push("");
    lines.push("## 10. Остановка и очистка");
    lines.push("");
    lines.push("Остановить, сохранив данные:");
    lines.push(starterFence("docker compose down", "bash"));
    lines.push("Остановить и **удалить все данные** (использовать только осознанно):");
    lines.push(starterFence("docker compose down -v", "bash"));
    lines.push("");
    lines.push("После `down -v` пароли и definitions будут применены заново при следующем старте.");
    lines.push("");
    lines.push("## 11. Что важно помнить");
    lines.push("");
    lines.push("- Это dev-стенд, а не HA-кластер.");
    lines.push("- Volume без backup не является стратегией восстановления.");
    lines.push("- Consumers и retry-логика живут в приложении, не в definitions.");
    lines.push("- Перед production нужны security review, нагрузочное тестирование и проверка DR.");
    lines.push("");
    return lines.join("\n") + "\n";
  }

  function buildAppsReadme(ctx) {
    const lines = [];
    lines.push("# Примеры producer и consumer");
    lines.push("");
    lines.push("Минимальные Node.js-примеры для топологии из этого пакета. Они **не объявляют**");
    lines.push("exchange/queue/binding заново — сначала поднимите брокер и импортируйте definitions.");
    lines.push("");
    lines.push("## Требования");
    lines.push("");
    lines.push("- Node.js 20+.");
    lines.push("- Запущенный RabbitMQ из корня пакета.");
    lines.push("- Установленный `amqplib` (`npm install`).");
    lines.push("");
    lines.push("## Запуск");
    lines.push(starterFence("npm install\nnpm run producer", "bash"));
    lines.push("В другом терминале:");
    lines.push(starterFence("npm run consumer", "bash"));
    lines.push("");
    lines.push("Если брокер запущен на других портах, задайте `RABBITMQ_URL`.");
    lines.push("");
    lines.push("**Bash:**");
    lines.push(starterFence("RABBITMQ_URL='amqp://rabbitlab:rabbitlab-dev@localhost:5672/%2F' npm run producer", "bash"));
    lines.push("**PowerShell:**");
    lines.push(starterFence("$env:RABBITMQ_URL='amqp://rabbitlab:rabbitlab-dev@localhost:5672/%2F'\nnpm run producer", "powershell"));
    lines.push("");
    lines.push("## Переменные окружения");
    lines.push("");
    lines.push("| Переменная | По умолчанию | Назначение |");
    lines.push("|---|---|---|");
    lines.push("| `RABBITMQ_URL` | `" + ctx.amqpUrl + "` | AMQP URL с vhost. `%2F` — URL-кодирование `/`. |");
    lines.push("| `EXCHANGE` | `" + ctx.exchangeName + "` | Exchange из definitions. |");
    lines.push("| `ROUTING_KEY` | `" + ctx.routingKey + "` | Ключ публикации. Для fanout игнорируется. |");
    lines.push("| `QUEUE` | `" + ctx.queueName + "` | Очередь для consumer. |");
    lines.push("| `PREFETCH` | `" + ctx.prefetch + "` | Максимум неподтверждённых сообщений. |");
    lines.push("| `PAYLOAD` | пример из пакета | Строка JSON для producer. |");
    lines.push("");
    lines.push("## Что делает producer");
    lines.push("");
    lines.push("- Создаёт confirm channel.");
    lines.push("- Публикует persistent JSON с `contentType`, `messageId` и headers.");
    lines.push("- Ждёт подтверждения брокера и печатает результат.");
    lines.push("- Не создаёт топологию: при отсутствии exchange получит channel error.");
    lines.push("");
    lines.push("## Что делает consumer");
    lines.push("");
    lines.push("- Открывает канал и выставляет prefetch.");
    lines.push("- Читает очередь с `noAck: false`.");
    lines.push("- Подтверждает сообщение через `ack` после имитации обработки.");
    lines.push("- При ошибке вызывает `nack(requeue=false)`: сообщение уйдёт в DLX или будет отброшено.");
    lines.push("");
    lines.push("Этот пример намеренно не реализует retry с backoff, idempotency key и graceful");
    lines.push("shutdown всего пула. Для production используйте проверенную библиотеку и добавьте");
    lines.push("таймауты, метрики, трассировку и ограничение числа ретраев.");
    lines.push("");
    lines.push("## Если сообщение не пришло");
    lines.push("");
    lines.push("- Проверьте, что definitions импортированы.");
    lines.push("- Сверьте exchange, routing key и bindings в `topology/topology.md`.");
    lines.push("- Для headers exchange проверьте `x-match` и заголовки.");
    lines.push("- Проверьте, что consumer слушает нужную очередь.");
    lines.push("");
    return lines.join("\n") + "\n";
  }

  function buildEnvExample() {
    return [
      "# rabbit.lab starter kit — локальные настройки",
      "# Скопируйте файл в .env и смените пароль перед использованием вне localhost.",
      "# .env не должен попадать в git.",
      "",
      "RABBITMQ_IMAGE=rabbitmq:3.13-management",
      "RABBITMQ_USER=rabbitlab",
      "RABBITMQ_PASS=rabbitlab-dev",
      "RABBITMQ_AMQP_PORT=5672",
      "RABBITMQ_MANAGEMENT_PORT=15672",
      "RABBITMQ_MANAGEMENT_URL=http://localhost:15672",
      "RABBITMQ_URL=amqp://rabbitlab:rabbitlab-dev@localhost:5672/%2F",
      "",
      "# Vhost в этом пакете фиксирован: /. Он указан в definitions.json и скриптах.",
      "# Пароль применяется при первом создании volume; для смены на существующем",
      "# volume используйте rabbitmqctl или пересоздайте volume.",
      "# Если меняете пользователя, пароль или порт, обновите RABBITMQ_URL и",
      "# RABBITMQ_MANAGEMENT_URL в этом же файле.",
      ""
    ].join("\n");
  }

  function buildDockerCompose() {
    return [
      "# rabbit.lab starter kit",
      "# Локальный RabbitMQ с management UI. Для production используйте TLS,",
      "# отдельного пользователя приложения, лимиты и внешний volume/backup.",
      "services:",
      "  rabbitmq:",
      "    image: \"${RABBITMQ_IMAGE:-rabbitmq:3.13-management}\"",
      "    hostname: rabbit-lab",
      "    restart: unless-stopped",
      "    ports:",
      "      - \"${RABBITMQ_AMQP_PORT:-5672}:5672\"",
      "      - \"${RABBITMQ_MANAGEMENT_PORT:-15672}:15672\"",
      "    environment:",
      "      # Эти переменные создают пользователя только на пустом volume.",
      "      # Пользователь нужен RabbitMQ до импорта definitions и для init-сервиса.",
      "      RABBITMQ_DEFAULT_USER: \"${RABBITMQ_USER:-rabbitlab}\"",
      "      RABBITMQ_DEFAULT_PASS: \"${RABBITMQ_PASS:-rabbitlab-dev}\"",
      "      RABBITMQ_DEFAULT_VHOST: \"/\"",
      "    volumes:",
      "      # Конфиг монтируется только для чтения. Definitions импортирует init-сервис.",
      "      - ./rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro",
      "      - ./rabbitmq/enabled_plugins:/etc/rabbitmq/enabled_plugins:ro",
      "      - rabbitmq-data:/var/lib/rabbitmq",
      "    healthcheck:",
      "      test: [\"CMD\", \"rabbitmq-diagnostics\", \"-q\", \"ping\"]",
      "      interval: 10s",
      "      timeout: 5s",
      "      retries: 12",
      "      start_period: 20s",
      "",
      "  # Одноразовый импорт topology. Не включаем management.load_definitions:",
      "  # при наличии definitions RabbitMQ не создаёт DEFAULT_USER и DEFAULT_VHOST.",
      "  rabbitmq-init:",
      "    image: \"${RABBITMQ_IMAGE:-rabbitmq:3.13-management}\"",
      "    depends_on:",
      "      rabbitmq:",
      "        condition: service_healthy",
      "    restart: \"no\"",
      "    environment:",
      "      RABBITMQ_USER: \"${RABBITMQ_USER:-rabbitlab}\"",
      "      RABBITMQ_PASS: \"${RABBITMQ_PASS:-rabbitlab-dev}\"",
      "    volumes:",
      "      - ./rabbitmq/definitions.json:/definitions.json:ro",
      "    entrypoint: [\"/bin/sh\", \"-c\"]",
      "    command:",
      "      - |",
      "        attempt=1",
      "        while [ \"$$attempt\" -le 30 ]; do",
      "          if rabbitmqadmin --host rabbitmq --port 15672 --username \"$$RABBITMQ_USER\" --password \"$$RABBITMQ_PASS\" import /definitions.json; then",
      "            echo \"definitions.json imported\"",
      "            exit 0",
      "          fi",
      "          echo \"management API is not ready (attempt $$attempt/30)\" >&2",
      "          attempt=$$((attempt + 1))",
      "          sleep 2",
      "        done",
      "        echo \"failed to import definitions.json\" >&2",
      "        exit 1",
      "",
      "volumes:",
      "  rabbitmq-data:",
      ""
    ].join("\n");
  }

  function buildRabbitmqConf() {
    return [
      "# rabbit.lab starter kit",
      "# AMQP listener внутри контейнера. Наружу порт публикует Docker Compose.",
      "listeners.tcp.default = 5672",
      "",
      "# Management UI и HTTP API. Нужны для проверки и apply-definitions.",
      "management.tcp.port = 15672",
      "",
      "# Definitions импортирует одноразовый Compose-сервис rabbitmq-init.",
      "# Не включайте management.load_definitions: при наличии definitions",
      "# RabbitMQ пропускает создание DEFAULT_USER/DEFAULT_VHOST из окружения.",
      "",
      "# Для production добавьте TLS, resource limits и настройки кластера.",
      "# Не публикуйте management UI в интернет без TLS и контроля доступа.",
      ""
    ].join("\n");
  }

  function buildEnabledPlugins() {
    return [
      "[rabbitmq_management].",
      ""
    ].join("\n");
  }

  function buildApplyDefinitionsPs() {
    return "\uFEFF" + [
      "# rabbit.lab starter kit — повторный импорт definitions через Management API.",
      "# Запуск из любой папки: powershell -ExecutionPolicy Bypass -File scripts/apply-definitions.ps1",
      "# Параметры можно передать явно, иначе берутся переменные окружения и dev-значения.",
      "param(",
      "  [string]$ManagementUrl = '',",
      "  [string]$User = '',",
      "  [string]$Password = ''",
      ")",
      "",
      "$ErrorActionPreference = 'Stop'",
      "if (-not $ManagementUrl) { $ManagementUrl = $env:RABBITMQ_MANAGEMENT_URL; if (-not $ManagementUrl) { $ManagementUrl = 'http://localhost:15672' } }",
      "if (-not $User) { $User = $env:RABBITMQ_USER; if (-not $User) { $User = 'rabbitlab' } }",
      "if (-not $Password) { $Password = $env:RABBITMQ_PASS; if (-not $Password) { $Password = 'rabbitlab-dev' } }",
      "$root = Split-Path -Parent $PSScriptRoot",
      "$definitions = Join-Path $root 'rabbitmq/definitions.json'",
      "if (-not (Test-Path $definitions)) { throw \"Не найден definitions.json: $definitions\" }",
      "",
      "$token = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(\"${User}:${Password}\"))",
      "$headers = @{ Authorization = \"Basic $token\" }",
      "$uri = $ManagementUrl.TrimEnd('/') + '/api/definitions'",
      "Write-Host \"POST $uri\"",
      "$result = Invoke-RestMethod -Method Post -Uri $uri -Headers $headers `",
      "  -ContentType 'application/json; charset=utf-8' `",
      "  -Body ([IO.File]::ReadAllText($definitions))",
      "Write-Host 'definitions.json применён. Проверьте exchanges/queues в management UI.'",
      ""
    ].join("\n");
  }

  function buildApplyDefinitionsSh() {
    return [
      "#!/usr/bin/env bash",
      "# rabbit.lab starter kit — повторный импорт definitions через Management API.",
      "set -euo pipefail",
      "",
      "ROOT=\"$(cd \"$(dirname \"${BASH_SOURCE[0]}\")/..\" && pwd)\"",
      "cd \"$ROOT\"",
      "",
      "# .env содержит локальные credentials. Не коммитьте его.",
      "if [ -f .env ]; then",
      "  set -a",
      "  # shellcheck disable=SC1091",
      "  . ./.env",
      "  set +a",
      "fi",
      "",
      ": \"${RABBITMQ_MANAGEMENT_URL:=http://localhost:15672}\"",
      ": \"${RABBITMQ_USER:=rabbitlab}\"",
      ": \"${RABBITMQ_PASS:=rabbitlab-dev}\"",
      "",
      "AUTH=\"$RABBITMQ_USER:$RABBITMQ_PASS\"",
      "URL=\"${RABBITMQ_MANAGEMENT_URL%/}/api/definitions\"",
      "",
      "echo \"POST $URL\"",
      "curl -fsS -u \"$AUTH\" \\",
      "  -H 'content-type: application/json' \\",
      "  --data-binary @rabbitmq/definitions.json \\",
      "  \"$URL\" > /dev/null",
      "echo 'definitions.json применён. Проверьте exchanges/queues в management UI.'",
      ""
    ].join("\n");
  }

  function buildSmokePs(ctx) {
    const queueNames = ctx.target.queues.map((queue) => starterPs(queue.name)).join(", ");
    const headerLines = Object.keys(ctx.headers).map((key) => "  " + starterPs(key) + " = " + starterPs(String(ctx.headers[key]).replace(/\r?\n/g, " ")));
    const messageHeaders = headerLines.length
      ? "$messageHeaders = @{\n" + headerLines.join("\n") + "\n}"
      : "$messageHeaders = @{}";
    const payload = starterPs(ctx.payload || "{}");
    return "\uFEFF" + [
      "# rabbit.lab starter kit — smoke-test management API и публикации.",
      "# Публикует реальное сообщение в dev-брокер. Запускайте только на локальном стенде.",
      "param(",
      "  [string]$ManagementUrl = '',",
      "  [string]$User = '',",
      "  [string]$Password = ''",
      ")",
      "",
      "$ErrorActionPreference = 'Stop'",
      "if (-not $ManagementUrl) { $ManagementUrl = $env:RABBITMQ_MANAGEMENT_URL; if (-not $ManagementUrl) { $ManagementUrl = 'http://localhost:15672' } }",
      "if (-not $User) { $User = $env:RABBITMQ_USER; if (-not $User) { $User = 'rabbitlab' } }",
      "if (-not $Password) { $Password = $env:RABBITMQ_PASS; if (-not $Password) { $Password = 'rabbitlab-dev' } }",
      "$base = $ManagementUrl.TrimEnd('/')",
      "$token = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(\"${User}:${Password}\"))",
      "$headers = @{ Authorization = \"Basic $token\" }",
      "$vhost = '%2F'",
      "",
      "$overview = Invoke-RestMethod -Uri \"$base/api/overview\" -Headers $headers",
      "Write-Host \"RabbitMQ: $($overview.rabbitmq_version)\"",
      "",
      "$queues = @(" + queueNames + ")",
      "foreach ($name in $queues) {",
      "  $path = [uri]::EscapeDataString($name)",
      "  $queue = Invoke-RestMethod -Uri \"$base/api/queues/$vhost/$path\" -Headers $headers",
      "  Write-Host (\"queue {0}: ready={1}, unacked={2}, consumers={3}\" -f $name, $queue.messages_ready, $queue.messages_unacknowledged, $queue.consumers)",
      "}",
      "",
      "$exchange = " + starterPs(ctx.exchangeName),
      "$routingKey = " + starterPs(ctx.routingKey),
      "$payload = " + payload,
      messageHeaders,
      "",
      "if ($exchange) {",
      "  $exchangePath = [uri]::EscapeDataString($exchange)",
      "  $body = @{",
      "    properties = @{ delivery_mode = 2; content_type = 'application/json'; headers = $messageHeaders }",
      "    routing_key = $routingKey",
      "    payload = $payload",
      "    payload_encoding = 'string'",
      "  } | ConvertTo-Json -Depth 8",
      "  $published = Invoke-RestMethod -Method Post -Uri \"$base/api/exchanges/$vhost/$exchangePath/publish\" -Headers $headers -ContentType 'application/json' -Body $body",
      "  Write-Host \"publish routed=$($published.routed)\"",
      "  if (-not $published.routed) {",
      "    Write-Error 'Сообщение не маршрутизировано. Проверьте bindings, routing key и headers в topology.md.'",
      "    exit 1",
      "  }",
      "} else {",
      "  Write-Host 'В модели нет exchange: публикация пропущена.'",
      "}",
      "",
      "Write-Host 'Smoke-test завершён.'",
      ""
    ].join("\n");
  }

  function buildSmokeSh(ctx) {
    const queueChecks = ctx.target.queues.map((queue) => {
      return "check_queue " + starterSh(queue.name) + " " + starterSh(encodeURIComponent(queue.name));
    }).join("\n");
    const body = JSON.stringify({
      properties: {
        delivery_mode: 2,
        content_type: "application/json",
        headers: ctx.headers
      },
      routing_key: ctx.routingKey,
      payload: ctx.payload,
      payload_encoding: "string"
    }, null, 2);
    return [
      "#!/usr/bin/env bash",
      "# rabbit.lab starter kit — smoke-test management API и публикации.",
      "# Публикует реальное сообщение в dev-брокер. Запускайте только на локальном стенде.",
      "set -euo pipefail",
      "",
      "ROOT=\"$(cd \"$(dirname \"${BASH_SOURCE[0]}\")/..\" && pwd)\"",
      "cd \"$ROOT\"",
      "",
      "if [ -f .env ]; then",
      "  set -a",
      "  # shellcheck disable=SC1091",
      "  . ./.env",
      "  set +a",
      "fi",
      "",
      ": \"${RABBITMQ_MANAGEMENT_URL:=http://localhost:15672}\"",
      ": \"${RABBITMQ_USER:=rabbitlab}\"",
      ": \"${RABBITMQ_PASS:=rabbitlab-dev}\"",
      "",
      "AUTH=\"$RABBITMQ_USER:$RABBITMQ_PASS\"",
      "BASE=\"${RABBITMQ_MANAGEMENT_URL%/}\"",
      "",
      "echo \"RabbitMQ management: $BASE\"",
      "curl -fsS -u \"$AUTH\" \"$BASE/api/overview\" > /dev/null",
      "echo 'management API: OK'",
      "",
      "check_queue() {",
      "  local name=\"$1\"",
      "  local encoded=\"$2\"",
      "  local code",
      "  code=$(curl -s -o /dev/null -w '%{http_code}' -u \"$AUTH\" \"$BASE/api/queues/%2F/$encoded\")",
      "  if [ \"$code\" != '200' ]; then",
      "    echo \"queue $name: HTTP $code (проверьте definitions)\" >&2",
      "    exit 1",
      "  fi",
      "  echo \"queue $name: OK\"",
      "}",
      "",
      queueChecks,
      "",
      "EXCHANGE_PATH=\"" + (ctx.exchangeName ? encodeURIComponent(ctx.exchangeName) : "") + "\"",
      "if [ -n \"$EXCHANGE_PATH\" ]; then",
      "  RESPONSE=$(curl -fsS -u \"$AUTH\" -H 'content-type: application/json' \\",
      "    -X POST \"$BASE/api/exchanges/%2F/$EXCHANGE_PATH/publish\" \\",
      "    --data-binary @- <<'JSON'",
      body,
      "JSON",
      "  )",
      "  printf '%s\\n' \"$RESPONSE\"",
      "  case \"$RESPONSE\" in",
      "    *'\"routed\":true'*) echo 'publish: routed=true' ;;",
      "    *) echo 'publish: routed=false — проверьте bindings, routing key и headers' >&2; exit 1 ;;",
      "  esac",
      "else",
      "  echo 'В модели нет exchange: публикация пропущена.'",
      "fi",
      "",
      "echo 'Smoke-test завершён.'",
      ""
    ].filter((line) => line !== null).join("\n");
  }

  function buildProducerSample(ctx) {
    const headers = JSON.stringify(ctx.headers || {}, null, 2);
    return [
      "// rabbit.lab starter kit — минимальный producer с publisher confirms.",
      "// Запуск: npm run producer",
      "// Топология уже должна быть импортирована из rabbitmq/definitions.json.",
      "",
      "import amqp from \"amqplib\";",
      "import { randomUUID } from \"node:crypto\";",
      "",
      "const url = process.env.RABBITMQ_URL ?? " + JSON.stringify(ctx.amqpUrl) + ";",
      "const exchange = process.env.EXCHANGE ?? " + JSON.stringify(ctx.exchangeName) + ";",
      "const routingKey = process.env.ROUTING_KEY ?? " + JSON.stringify(ctx.routingKey) + ";",
      "const payload = process.env.PAYLOAD ?? " + JSON.stringify(ctx.payload || "{}") + ";",
      "const messageHeaders = " + headers + ";",
      "",
      "let connection;",
      "",
      "try {",
      "  connection = await amqp.connect(url);",
      "  const channel = await connection.createConfirmChannel();",
      "",
      "  // persistent + confirm channel: сообщение сохраняется на durable queue",
      "  // после подтверждения брокером. Это не гарантирует обработку consumer.",
      "  channel.publish(exchange, routingKey, Buffer.from(payload), {",
      "    persistent: true,",
      "    contentType: \"application/json\",",
      "    messageId: randomUUID(),",
      "    timestamp: Math.floor(Date.now() / 1000),",
      "    headers: messageHeaders",
      "  });",
      "",
      "  await channel.waitForConfirms();",
      "  console.log(JSON.stringify({",
      "    event: \"published\",",
      "    exchange: exchange || \"(default)\",",
      "    routingKey: routingKey,",
      "    bytes: Buffer.byteLength(payload)",
      "  }));",
      "",
      "  await channel.close();",
      "} catch (error) {",
      "  console.error(\"producer failed:\", error.message);",
      "  process.exitCode = 1;",
      "} finally {",
      "  if (connection) await connection.close().catch(() => {});",
      "}",
      ""
    ].join("\n");
  }

  function buildConsumerSample(ctx) {
    return [
      "// rabbit.lab starter kit — минимальный consumer с prefetch и ручным ack.",
      "// Запуск: npm run consumer",
      "// Топология уже должна быть импортирована из rabbitmq/definitions.json.",
      "",
      "import amqp from \"amqplib\";",
      "",
      "const url = process.env.RABBITMQ_URL ?? " + JSON.stringify(ctx.amqpUrl) + ";",
      "const queue = process.env.QUEUE ?? " + JSON.stringify(ctx.queueName) + ";",
      "const prefetch = Number(process.env.PREFETCH ?? " + JSON.stringify(String(ctx.prefetch)) + ");",
      "",
      "let connection;",
      "let channel;",
      "let consumerTag;",
      "let closing = false;",
      "",
      "async function shutdown() {",
      "  if (closing) return;",
      "  closing = true;",
      "  try {",
      "    if (channel && consumerTag) await channel.cancel(consumerTag);",
      "    if (channel) await channel.close();",
      "  } catch (error) {",
      "    console.error(\"shutdown channel:\", error.message);",
      "  }",
      "  if (connection) await connection.close().catch(() => {});",
      "}",
      "",
      "try {",
      "  connection = await amqp.connect(url);",
      "  channel = await connection.createChannel();",
      "  await channel.prefetch(Number.isFinite(prefetch) && prefetch > 0 ? prefetch : 1);",
      "",
      "  const result = await channel.consume(queue, (message) => {",
      "    if (!message) return;",
      "    try {",
      "      const text = message.content.toString(\"utf8\");",
      "      // Здесь должна быть реальная обработка. Имитация специально короткая.",
      "      console.log(JSON.stringify({",
      "        event: \"received\",",
      "        queue: queue,",
      "        routingKey: message.fields.routingKey,",
      "        headers: message.properties.headers ?? {},",
      "        payload: text",
      "      }));",
      "      channel.ack(message);",
      "    } catch (error) {",
      "      // requeue=false: сообщение уйдёт в DLX, если он настроен, иначе будет отброшено.",
      "      console.error(\"consumer handler failed:\", error.message);",
      "      channel.nack(message, false, false);",
      "    }",
      "  }, { noAck: false });",
      "",
      "  consumerTag = result.consumerTag;",
      "  console.log(JSON.stringify({ event: \"consuming\", queue: queue, prefetch: prefetch }));",
      "",
      "  process.on(\"SIGINT\", async () => {",
      "    await shutdown();",
      "    process.exit(0);",
      "  });",
      "  process.on(\"SIGTERM\", async () => {",
      "    await shutdown();",
      "    process.exit(0);",
      "  });",
      "} catch (error) {",
      "  console.error(\"consumer failed:\", error.message);",
      "  await shutdown();",
      "  process.exitCode = 1;",
      "}",
      ""
    ].join("\n");
  }

  function buildAppsPackage() {
    return JSON.stringify({
      name: "rabbit-lab-starter-apps",
      private: true,
      version: "1.0.0",
      type: "module",
      description: "Минимальные producer/consumer примеры для rabbit.lab starter kit",
      engines: { node: ">=20" },
      scripts: {
        producer: "node producer.js",
        consumer: "node consumer.js"
      },
      dependencies: {
        amqplib: "^0.10.5"
      }
    }, null, 2) + "\n";
  }

  function buildStarterKit() {
    const ctx = starterContext();
    const files = {};
    files["README.md"] = "";
    files[".env.example"] = buildEnvExample();
    files["docker-compose.yml"] = buildDockerCompose();
    files["rabbitmq/definitions.json"] = JSON.stringify(buildDefinitions(), null, 2) + "\n";
    files["rabbitmq/rabbitmq.conf"] = buildRabbitmqConf();
    files["rabbitmq/enabled_plugins"] = buildEnabledPlugins();
    files["topology/topology.md"] = buildTopologyMarkdown(ctx);
    files["topology/topology.mmd"] = buildTopologyMermaid();
    files["topology/topology.json"] = JSON.stringify(buildTopologyModel(), null, 2) + "\n";
    files["project/rabbit-lab-project.json"] = JSON.stringify(buildProject(), null, 2) + "\n";
    files["scripts/apply-definitions.ps1"] = buildApplyDefinitionsPs();
    files["scripts/apply-definitions.sh"] = buildApplyDefinitionsSh();
    files["scripts/smoke-test.ps1"] = buildSmokePs(ctx);
    files["scripts/smoke-test.sh"] = buildSmokeSh(ctx);
    files["apps/README.md"] = buildAppsReadme(ctx);
    files["apps/package.json"] = buildAppsPackage();
    files["apps/producer.js"] = buildProducerSample(ctx);
    files["apps/consumer.js"] = buildConsumerSample(ctx);
    files["docs/architecture.md"] = buildArchitectureDoc(ctx);
    files["docs/operations.md"] = buildOperationsDoc(ctx);
    files["README.md"] = buildStarterReadme(ctx, files);
    return files;
  }

  let starterCrcTable = null;

  function starterCrc32(bytes) {
    if (!starterCrcTable) {
      starterCrcTable = new Uint32Array(256);
      for (let index = 0; index < 256; index += 1) {
        let value = index;
        for (let bit = 0; bit < 8; bit += 1) {
          value = (value & 1) ? (0xedb88320 ^ (value >>> 1)) : (value >>> 1);
        }
        starterCrcTable[index] = value >>> 0;
      }
    }
    let crc = 0xffffffff;
    for (let index = 0; index < bytes.length; index += 1) {
      crc = (crc >>> 8) ^ starterCrcTable[(crc ^ bytes[index]) & 0xff];
    }
    return (crc ^ 0xffffffff) >>> 0;
  }

  function buildStarterZipBlob(files, rootName) {
    const encoder = new TextEncoder();
    const localParts = [];
    const centralParts = [];
    const now = new Date();
    const dosTime = (now.getHours() << 11) | (now.getMinutes() << 5) | Math.floor(now.getSeconds() / 2);
    const dosDate = ((now.getFullYear() - 1980) << 9) | ((now.getMonth() + 1) << 5) | now.getDate();
    const root = String(rootName || STARTER_ROOT).replace(/\/+$/, "");
    let offset = 0;

    Object.keys(files).sort().forEach((relativePath) => {
      const name = root + "/" + relativePath;
      const nameBytes = encoder.encode(name);
      const dataBytes = encoder.encode(String(files[relativePath]));
      const crc = starterCrc32(dataBytes);

      const local = new Uint8Array(30 + nameBytes.length);
      const localView = new DataView(local.buffer);
      localView.setUint32(0, 0x04034b50, true);
      localView.setUint16(4, 20, true);
      localView.setUint16(6, 0x0800, true);
      localView.setUint16(8, 0, true);
      localView.setUint16(10, dosTime, true);
      localView.setUint16(12, dosDate, true);
      localView.setUint32(14, crc, true);
      localView.setUint32(18, dataBytes.length, true);
      localView.setUint32(22, dataBytes.length, true);
      localView.setUint16(26, nameBytes.length, true);
      localView.setUint16(28, 0, true);
      local.set(nameBytes, 30);
      localParts.push(local, dataBytes);

      const central = new Uint8Array(46 + nameBytes.length);
      const centralView = new DataView(central.buffer);
      centralView.setUint32(0, 0x02014b50, true);
      centralView.setUint16(4, 20, true);
      centralView.setUint16(6, 20, true);
      centralView.setUint16(8, 0x0800, true);
      centralView.setUint16(10, 0, true);
      centralView.setUint16(12, dosTime, true);
      centralView.setUint16(14, dosDate, true);
      centralView.setUint32(16, crc, true);
      centralView.setUint32(20, dataBytes.length, true);
      centralView.setUint32(24, dataBytes.length, true);
      centralView.setUint16(28, nameBytes.length, true);
      centralView.setUint16(30, 0, true);
      centralView.setUint16(32, 0, true);
      centralView.setUint16(34, 0, true);
      centralView.setUint16(36, 0, true);
      centralView.setUint32(38, 0, true);
      centralView.setUint32(42, offset, true);
      central.set(nameBytes, 46);
      centralParts.push(central);

      offset += local.length + dataBytes.length;
    });

    const centralSize = centralParts.reduce((sum, part) => sum + part.length, 0);
    const end = new Uint8Array(22);
    const endView = new DataView(end.buffer);
    endView.setUint32(0, 0x06054b50, true);
    endView.setUint16(4, 0, true);
    endView.setUint16(6, 0, true);
    endView.setUint16(8, centralParts.length, true);
    endView.setUint16(10, centralParts.length, true);
    endView.setUint32(12, centralSize, true);
    endView.setUint32(16, offset, true);
    endView.setUint16(20, 0, true);

    return new Blob(localParts.concat(centralParts, [end]), { type: "application/zip" });
  }

  function downloadBlob(filename, blob) {
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1600);
  }

  function starterZipName() {
    return "rabbit-lab-starter.zip";
  }

  function exportStarterKit() {
    try {
      const files = buildStarterKit();
      const blob = buildStarterZipBlob(files);
      downloadBlob(starterZipName(), blob);
      addLog("sys", "export · starter kit · " + Object.keys(files).length + " files · " + blob.size + " bytes");
      closeModal(el.exportModal);
      renderLog();
      persistSoon();
      toast("Starter kit скачан: " + Object.keys(files).length + " файлов, README, Compose, definitions и примеры.", "Экспорт");
    } catch (error) {
      toast("Не удалось собрать архив: " + error.message, "Ошибка экспорта");
    }
  }

  function renderExportManifest() {
    const files = buildStarterKit();
    const blob = buildStarterZipBlob(files);
    const size = Math.max(1, Math.round(blob.size / 1024));
    const summary = document.getElementById("sim-export-summary");
    const count = document.getElementById("sim-export-count");
    const tree = document.getElementById("sim-export-tree");
    const browser = buildStarterBrowserRows(files);
    const folders = browser.rows.filter((row) => row.kind === "dir").length;
    const meta = Object.keys(files).length + " файлов · " + folders + " разделов · " + size + " КБ";
    if (summary) summary.textContent = meta + " · ZIP · сценарий " + scenarioOfCurrent().label;
    if (count) count.textContent = meta;
    if (tree) tree.innerHTML = renderStarterBrowser(files);
  }

  /* ---------- interaction ---------- */

  let drag = null;
  let pan = null;
  let resizeTimer = null;

  function pointerWorld(event) {
    const rect = el.canvas.getBoundingClientRect();
    return {
      x: (event.clientX - rect.left - view.x) / view.scale,
      y: (event.clientY - rect.top - view.y) / view.scale
    };
  }

  function modelFor(kind, id) {
    const target = state();
    return kind === "queue" ? queueById(target, id) : kind === "exchange" ? exchangeById(target, id) : kind === "producer" ? producerById(target, id) : consumerById(target, id);
  }

  function onWindowPointerMove(event) {
    if (drag) {
      const dx = (event.clientX - drag.startX) / view.scale;
      const dy = (event.clientY - drag.startY) / view.scale;
      if (Math.abs(dx) > 2 || Math.abs(dy) > 2) drag.moved = true;
      const model = modelFor(drag.kind, drag.id);
      if (!model) return;
      model.x = clamp(Math.round(drag.origX + dx), 0, 4000);
      model.y = clamp(Math.round(drag.origY + dy), 0, 3000);
      drag.box.style.left = model.x + "px";
      drag.box.style.top = model.y + "px";
      drawEdges();
    } else if (pan) {
      view.x = pan.viewX + (event.clientX - pan.startX);
      view.y = pan.viewY + (event.clientY - pan.startY);
      if (Math.abs(event.clientX - pan.startX) > 2 || Math.abs(event.clientY - pan.startY) > 2) pan.moved = true;
      applyView();
    } else if (ui.connect) {
      const point = pointerWorld(event);
      ui.connect.x2 = point.x;
      ui.connect.y2 = point.y;
      drawEdges();
    }
  }

  function onWindowPointerUp(event) {
    if (drag) {
      const model = modelFor(drag.kind, drag.id);
      if (model) {
        if (drag.moved) {
          model.x = Math.round(model.x / GRID) * GRID;
          model.y = Math.round(model.y / GRID) * GRID;
          recordHistory(drag.snapshot);
          rememberView();
          addLog("sys", "node moved · " + drag.kind + " " + (model.name || drag.id) + " · " + model.x + "," + model.y);
          ui.selected = { kind: drag.kind, id: drag.id };
          renderAll();
          persistSoon();
        } else {
          selectNode(drag.kind, drag.id);
        }
      }
      if (drag.box) drag.box.classList.remove("is-dragging");
      drag = null;
      ui.busy = false;
    } else if (pan) {
      if (!pan.moved) {
        ui.selected = null;
        renderNodes();
        renderInspector();
      }
      rememberView();
      el.canvas.classList.remove("is-panning");
      pan = null;
      ui.busy = false;
      persistSoon();
    } else if (ui.connect) {
      const targetEl = document.elementFromPoint(event.clientX, event.clientY);
      const box = targetEl ? targetEl.closest(".sim-node") : null;
      const from = ui.connect;
      ui.connect = null;
      drawEdges();
      ui.busy = false;
      if (box) {
        const toKind = box.getAttribute("data-node-kind");
        const toId = box.getAttribute("data-node-id");
        if (!(toKind === from.fromKind && toId === from.fromId)) connectNodes(from.fromKind, from.fromId, toKind, toId);
      }
    }
    window.removeEventListener("pointermove", onWindowPointerMove);
    window.removeEventListener("pointerup", onWindowPointerUp);
    window.removeEventListener("pointercancel", onWindowPointerUp);
  }

  function onCanvasPointerDown(event) {
    if (event.button !== 0) return;
    const target = event.target;
    const port = target.closest(".sim-node-port");
    if (port) {
      if (port.getAttribute("data-port") !== "out") {
        toast("Тяните связь от правого порта узла.", "Направление связи");
        return;
      }
      const box = port.closest(".sim-node");
      if (!box) return;
      const kind = box.getAttribute("data-node-kind");
      const id = box.getAttribute("data-node-id");
      const point = pointerWorld(event);
      const startX = box.offsetLeft + box.offsetWidth;
      const startY = box.offsetTop + box.offsetHeight / 2;
      ui.connect = { fromKind: kind, fromId: id, x1: startX, y1: startY, x2: point.x, y2: point.y };
      ui.busy = true;
      drawEdges();
      event.preventDefault();
      window.addEventListener("pointermove", onWindowPointerMove);
      window.addEventListener("pointerup", onWindowPointerUp);
      window.addEventListener("pointercancel", onWindowPointerUp);
      return;
    }

    const node = target.closest(".sim-node");
    if (node && !target.closest("button, a, input, select, textarea, .sim-node-port")) {
      const kind = node.getAttribute("data-node-kind");
      const id = node.getAttribute("data-node-id");
      const model = modelFor(kind, id);
      if (!model) return;
      drag = {
        kind: kind,
        id: id,
        box: node,
        startX: event.clientX,
        startY: event.clientY,
        origX: Number(model.x) || 0,
        origY: Number(model.y) || 0,
        moved: false,
        snapshot: snapshotTopology()
      };
      node.classList.add("is-dragging");
      ui.busy = true;
      event.preventDefault();
      window.addEventListener("pointermove", onWindowPointerMove);
      window.addEventListener("pointerup", onWindowPointerUp);
      window.addEventListener("pointercancel", onWindowPointerUp);
      return;
    }

    if (target.closest(".sim-canvas-controls")) return;
    if (target !== el.canvas && target !== el.stage && target !== el.nodes && target !== el.edges) return;
    pan = {
      startX: event.clientX,
      startY: event.clientY,
      viewX: view.x,
      viewY: view.y,
      moved: false
    };
    ui.busy = true;
    el.canvas.classList.add("is-panning");
    event.preventDefault();
    window.addEventListener("pointermove", onWindowPointerMove);
    window.addEventListener("pointerup", onWindowPointerUp);
    window.addEventListener("pointercancel", onWindowPointerUp);
  }

  function onWheel(event) {
    event.preventDefault();
    const rect = el.canvas.getBoundingClientRect();
    const mouseX = event.clientX - rect.left;
    const mouseY = event.clientY - rect.top;
    const worldX = (mouseX - view.x) / view.scale;
    const worldY = (mouseY - view.y) / view.scale;
    const factor = event.deltaY < 0 ? 1.12 : 0.89;
    const next = clamp(view.scale * factor, 0.25, 2.4);
    view.scale = next;
    view.x = mouseX - worldX * next;
    view.y = mouseY - worldY * next;
    applyView();
    rememberView();
    persistSoon();
  }

  function zoomBy(factor) {
    const width = el.canvas.clientWidth;
    const height = el.canvas.clientHeight;
    const worldX = (width / 2 - view.x) / view.scale;
    const worldY = (height / 2 - view.y) / view.scale;
    view.scale = clamp(view.scale * factor, 0.25, 2.4);
    view.x = width / 2 - worldX * view.scale;
    view.y = height / 2 - worldY * view.scale;
    applyView();
    rememberView();
    persistSoon();
  }

  function handleAction(name, button) {
    const target = state();
    switch (name) {
      case "import": el.importFile.click(); break;
      case "export": openExport(); break;
      case "new-empty": newEmpty(); break;
      case "add-binding":
        openBindingModal({ exchange: button.getAttribute("data-exchange") || "", queue: button.getAttribute("data-queue") || "" });
        break;
      case "fit":
        fitView();
        rememberView();
        persistSoon();
        break;
      case "undo": undo(); break;
      case "redo": redo(); break;
      case "inspector":
        if (ui.selected) setTab("props");
        else setTab("health");
        openInspector();
        break;
      case "close-inspector": closeInspector(); break;
      case "palette":
        el.sidebar.classList.toggle("is-open");
        break;
      case "reset": resetCurrent(); break;
      case "check": attemptCheck(); break;
      case "clear-log":
        target.log = [];
        renderLog();
        persistSoon();
        break;
      case "trace": traceRoute(); break;
      case "pause": togglePause(); break;
      case "zoom-in": zoomBy(1.16); break;
      case "zoom-out": zoomBy(1 / 1.16); break;
      case "publish-node": publishFromProducer(button.getAttribute("data-id")); break;
      case "add-consumer": addConsumer(button.getAttribute("data-queue")); break;
      case "toggle-consumer": toggleConsumer(button.getAttribute("data-id")); break;
      case "select-node": selectNode(button.getAttribute("data-kind"), button.getAttribute("data-id")); break;
      case "delete-node": deleteNode(button.getAttribute("data-kind"), button.getAttribute("data-id")); break;
      case "remove-binding": removeBinding(button.getAttribute("data-id")); break;
      case "purge-queue": purgeQueue(button.getAttribute("data-id")); break;
      case "message-ack": {
        const queue = queueById(target, button.getAttribute("data-queue"));
        if (queue) ackMessage(queue, button.getAttribute("data-id"));
        break;
      }
      case "message-nack": {
        const queue = queueById(target, button.getAttribute("data-queue"));
        if (queue) nackMessage(queue, button.getAttribute("data-id"));
        break;
      }
      case "message-requeue": {
        const queue = queueById(target, button.getAttribute("data-queue"));
        if (queue) requeueMessage(queue, button.getAttribute("data-id"));
        break;
      }
      case "message-reject": {
        const queue = queueById(target, button.getAttribute("data-queue"));
        if (queue) rejectMessage(queue, button.getAttribute("data-id"));
        break;
      }
      case "health-fix": attemptHealthFix(button); break;
      case "docs":
        window.open("https://www.rabbitmq.com/tutorials/amqp-concepts", "_blank", "noopener,noreferrer");
        break;
      default:
        break;
    }
  }

  function onClick(event) {
    const scenarioButton = event.target.closest("[data-scenario]");
    if (scenarioButton) {
      loadScenario(scenarioButton.getAttribute("data-scenario"));
      el.sidebar.classList.remove("is-open");
      return;
    }
    const addButton = event.target.closest("[data-add-node]");
    if (addButton) {
      const rect = el.canvas.getBoundingClientRect();
      const kind = addButton.getAttribute("data-add-node");
      const target = state();
      const count = kind === "queue" ? target.queues.length : kind === "exchange" ? target.exchanges.length : kind === "producer" ? target.producers.length : target.consumers.length;
      const column = { producer: -300, exchange: -40, queue: 280, consumer: 520 }[kind] || 0;
      const point = {
        x: (rect.width * 0.5 - view.x) / view.scale + column,
        y: (rect.height * 0.38 - view.y) / view.scale + (count % 5) * 80
      };
      addNode(kind, point.x, point.y);
      el.sidebar.classList.remove("is-open");
      return;
    }
    const exportDetailButton = event.target.closest("[data-export-detail]");
    if (exportDetailButton) {
      openStarterDetail(
        exportDetailButton.getAttribute("data-detail-kind"),
        exportDetailButton.getAttribute("data-export-detail")
      );
      return;
    }
    const exportPartButton = event.target.closest("[data-export-part]");
    if (exportPartButton) {
      exportStarterFile(exportPartButton.getAttribute("data-export-part"));
      return;
    }
    const exportFolderButton = event.target.closest("[data-export-folder]");
    if (exportFolderButton) {
      exportStarterFolder(exportFolderButton.getAttribute("data-export-folder"));
      return;
    }
    const exportDetailDownload = event.target.closest("[data-export-detail-download]");
    if (exportDetailDownload) {
      downloadStarterSelection(
        exportDetailDownload.getAttribute("data-kind"),
        exportDetailDownload.getAttribute("data-path")
      );
      return;
    }
    if (event.target.closest("[data-close-export-detail]")) {
      closeStarterDetail();
      return;
    }
    const exportButton = event.target.closest("[data-export]");
    if (exportButton) {
      const type = exportButton.getAttribute("data-export");
      if (type === "starter") exportStarterKit();
      else if (type === "definitions") exportDefinitions();
      else if (type === "project") exportProject();
      else if (type === "markdown") exportMarkdown();
      return;
    }
    if (event.target.closest("[data-close-sim]")) {
      closeModals();
      return;
    }
    if (event.target.closest("[data-sim-help]")) {
      openHelp();
      return;
    }
    if (event.target.closest("[data-sim-save]")) {
      persistNow(true);
      return;
    }
    if (event.target.classList && event.target.classList.contains("sim-modal")) {
      closeModal(event.target);
      return;
    }
    const actionButton = event.target.closest("[data-action]");
    if (actionButton) {
      handleAction(actionButton.getAttribute("data-action"), actionButton);
    }
  }

  function onChange(event) {
    const field = event.target.closest("[data-field]");
    if (!field) return;
    const kind = field.getAttribute("data-kind");
    const id = field.getAttribute("data-id");
    const key = field.getAttribute("data-field");
    let value;
    if (field.type === "checkbox") value = field.checked;
    else value = field.value;
    updateNode(kind, id, key, value);
  }

  function onPublishInput() {
    const target = state();
    target.pub.key = el.pubKey.value;
    target.pub.payload = el.pubPayload.value;
    target.pub.headers = el.pubHeaders.value;
    target.pub.count = clamp(Number(el.pubCount.value) || 1, 1, 100);
    if (producerSource(target)) target.pub.source = producerSource(target).id;
    persistSoon();
  }

  function onPublishSourceChange() {
    const target = state();
    const producer = producerById(target, el.pubSource.value);
    if (!producer) return;
    target.pub.source = producer.id;
    if (producer.defaultKey) target.pub.key = producer.defaultKey;
    syncPublishForm(true);
    setRoutePreview((target.pub.key || "(no key)") + " → нажмите «показать маршрут»", "");
    persistSoon();
  }

  function onAutosendChange() {
    if (el.autosend.checked) startAutosend();
    else stopAutosend();
  }

  function onAutosendSeconds() {
    if (autosendTimer) startAutosend();
  }

  function onKeyDown(event) {
    const active = document.activeElement;
    const typing = active && (active.tagName === "INPUT" || active.tagName === "TEXTAREA" || active.tagName === "SELECT" || active.isContentEditable);
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      publishFromForm();
      return;
    }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "z" && !typing) {
      event.preventDefault();
      undo();
      return;
    }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "y" && !typing) {
      event.preventDefault();
      redo();
      return;
    }
    if (event.key === "Escape") {
      const exportDetail = document.getElementById("sim-modal-export-detail");
      if (exportDetail && exportDetail.classList.contains("is-open")) {
        closeStarterDetail();
        return;
      }
      if ($(".sim-modal.is-open")) {
        closeModals();
        return;
      }
      if (ui.connect) {
        ui.connect = null;
        drawEdges();
        ui.busy = false;
        return;
      }
      if (ui.selected) {
        ui.selected = null;
        renderNodes();
        renderInspector();
      }
      return;
    }
    if ((event.key === "Delete" || event.key === "Backspace") && !typing && ui.selected) {
      event.preventDefault();
      deleteNode(ui.selected.kind, ui.selected.id);
    }
  }

  function bindEvents() {
    el.canvas.addEventListener("pointerdown", onCanvasPointerDown);
    el.canvas.addEventListener("wheel", onWheel, { passive: false });
    document.addEventListener("click", onClick);
    document.addEventListener("change", (event) => {
      if (event.target.closest("#sim-inspector-content")) onChange(event);
    });
    document.addEventListener("keydown", onKeyDown);
    el.tabs.forEach((button) => {
      button.addEventListener("click", () => {
        openInspector();
        setTab(button.getAttribute("data-insp-tab"));
      });
    });
    el.bindingForm.addEventListener("submit", submitBinding);
    el.bindingExchange.addEventListener("change", updateBindingForm);
    el.importFile.addEventListener("change", () => importDefinitions(el.importFile.files[0]));
    el.publishBtn.addEventListener("click", () => publishFromForm());
    el.pubSource.addEventListener("change", onPublishSourceChange);
    [el.pubKey, el.pubPayload, el.pubHeaders, el.pubCount].forEach((field) => {
      field.addEventListener("input", onPublishInput);
    });
    el.autosend.addEventListener("change", onAutosendChange);
    el.autosendSec.addEventListener("change", onAutosendSeconds);
    el.speed.addEventListener("change", () => setSpeed(el.speed.value));
    el.scenarioSelect.addEventListener("change", () => loadScenario(el.scenarioSelect.value));
    window.addEventListener("resize", () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => {
        renderNodes();
        const saved = state().view;
        const width = el.canvas.clientWidth;
        const refit = window.innerWidth <= 860 || !saved || !Number(saved.w) || Math.abs(Number(saved.w) - width) > Math.max(90, width * 0.24);
        if (refit) fitView();
        else applyView();
      }, 180);
    });
    window.addEventListener("hashchange", () => {
      const id = window.location.hash.replace(/^#/, "");
      if (SCENARIOS.some((scenario) => scenario.id === id) && id !== currentId) loadScenario(id);
    });
    window.addEventListener("beforeunload", () => persistNow(false));
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "hidden") persistNow(false);
    });
  }

  /* ---------- boot ---------- */

  function updateClock() {
    const node = $("#sim-timer");
    if (!node) return;
    clockSeconds -= 1;
    if (clockSeconds < 0) clockSeconds = 59 * 60 + 59;
    node.textContent = pad(Math.floor(clockSeconds / 60)) + ":" + pad(clockSeconds % 60);
  }

  function boot() {
    if (booted) return;
    cacheDom();
    restoreSaved();
    bindEvents();
    booted = true;
    const initial = getScenario(currentId);
    if (!states[currentId]) states[currentId] = createState(initial);
    loadScenario(currentId);
    updateClock();
    setInterval(updateClock, 1000);
    setInterval(tickSimulation, 1000);
    window.rabbitLabSim = {
      get state() { return state(); },
      get currentId() { return currentId; },
      loadScenario: loadScenario,
      checkResults: checkResults,
      attemptCheck: attemptCheck,
      auditTopology: auditTopology,
      publishFromForm: publishFromForm,
      publishFromProducer: publishFromProducer,
      exportDefinitions: exportDefinitions,
      buildDefinitions: buildDefinitions,
      buildProject: buildProject,
      buildTopologyMarkdown: buildTopologyMarkdown,
      buildTopologyMermaid: buildTopologyMermaid,
      buildTopologyModel: buildTopologyModel,
      buildStarterKit: buildStarterKit,
      buildStarterZipBlob: buildStarterZipBlob,
      buildStarterTreeData: buildStarterTreeData,
      exportStarterKit: exportStarterKit,
      exportStarterFile: exportStarterFile,
      exportStarterFolder: exportStarterFolder,
      openStarterDetail: openStarterDetail,
      closeStarterDetail: closeStarterDetail,
      renderExportManifest: renderExportManifest,
      applyImportedData: applyImportedData,
      resetCurrent: resetCurrent,
      addNode: addNode,
      addConsumer: addConsumer,
      toggleConsumer: toggleConsumer,
      connectNodes: connectNodes,
      updateNode: updateNode,
      deleteNode: deleteNode,
      removeBinding: removeBinding,
      purgeQueue: purgeQueue,
      undo: undo,
      redo: redo
    };
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();

