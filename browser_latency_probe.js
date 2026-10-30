/*
 * VERSION 2 — ĐO ĐỘ TRỄ TRONG PHIÊN IOC ĐANG MỞ, không đọc token/cookie.
 * Chuẩn bị: mở riêng camera REPLACE_WITH_CAMERA_CODE ở một ô, bật AI, giữ tab ở phía trước.
 * Dán toàn bộ file vào DevTools Console, đợi 60 giây.
 * Kết quả: window.__iocLatencyProbe.result (chỉ số đo, không ảnh/bbox/token).
 * Có thể dừng sớm: window.__iocLatencyProbe.stop().
 * Copy file bằng PowerShell: Get-Content -Raw -Encoding UTF8 <đường dẫn file> | Set-Clipboard
 * Copy kết quả bằng lệnh Console: copy(JSON.stringify(window.__iocLatencyProbe.result, null, 2)).
 *
 * Luồng: dùng lại module/WS của app; mở MQTT chỉ subscribe bbox cùng camera,
 * ghép cùng timestamp để đo MQTT→WS bằng một đồng hồ, không bị clock skew.
 * Sau đó đo canvas strokeRect
 * khớp tọa độ mới → lấy timestamp frame video nếu browser hỗ trợ → tự phục hồi
 * hook canvas khi kết thúc. Không sửa server, FPS, tọa độ hoặc dữ liệu camera.
 * Tuổi publish→browser dùng hai đồng hồ, chưa hiệu chỉnh clock skew: chỉ tham khảo.
 */
function iocOpenMqttObserver(url, onPayload, onState) {
  const encoder = new TextEncoder(), decoder = new TextDecoder();
  const str = text => { const b = encoder.encode(text); return [b.length >> 8, b.length & 255, ...b]; };
  const ws = new WebSocket(url, "mqtt"); ws.binaryType = "arraybuffer";
  let buffer = new Uint8Array(0), closed = false;
  const send = (header, payload = []) => {
    let size = payload.length; const length = [];
    do { let b = size % 128; size = Math.floor(size / 128); if (size) b |= 128; length.push(b); } while (size);
    if (ws.readyState === WebSocket.OPEN) ws.send(new Uint8Array([header, ...length, ...payload]));
  };
  ws.onopen = () => {
    // MQTT3.1.1 CONNECT, clean session, tài khoản broker theo yêu cầu dự án.
    send(0x10, [0, 4, 77, 81, 84, 84, 4, 0xc2, 0, 30,
      ...str("tts_browser_" + Math.random().toString(16).slice(2)), ...str("REPLACE_WITH_MQTT_USERNAME"), ...str("REPLACE_WITH_MQTT_PASSWORD")]);
  };
  ws.onmessage = event => {
    const incoming = new Uint8Array(event.data), joined = new Uint8Array(buffer.length + incoming.length);
    joined.set(buffer); joined.set(incoming, buffer.length); buffer = joined;
    if (buffer.length > 1024 * 1024) { onState("packet_too_large"); ws.close(); return; }
    while (buffer.length >= 2) {
      let pos = 1, length = 0, multiplier = 1, byte;
      do {
        if (pos >= buffer.length) return;
        byte = buffer[pos++]; length += (byte & 127) * multiplier; multiplier *= 128;
        if (multiplier > 128 ** 4) { ws.close(); return; }
      } while (byte & 128);
      if (buffer.length < pos + length) return;
      const type = buffer[0] >> 4, qos = (buffer[0] >> 1) & 3;
      const body = buffer.slice(pos, pos + length); buffer = buffer.slice(pos + length);
      if (type === 2) {
        onState(body[1] === 0 ? "mqtt_connected" : "mqtt_rejected_" + body[1]);
        if (body[1] === 0) send(0x82, [0, 1, ...str("smart_vms/ai/bbox/REPLACE_WITH_CAMERA_CODE"), 0]);
      } else if (type === 9) {
        onState(body[2] < 128 ? "mqtt_subscribed" : "mqtt_subscribe_rejected");
      } else if (type === 3 && body.length >= 2) {
        const topicLength = body[0] * 256 + body[1]; let offset = 2 + topicLength;
        if (qos) { if (qos === 1) send(0x40, [body[offset], body[offset + 1]]); offset += 2; }
        try { onPayload(JSON.parse(decoder.decode(body.slice(offset)))); } catch { onState("mqtt_payload_parse_error"); }
      }
    }
  };
  ws.onerror = () => onState("mqtt_websocket_error");
  ws.onclose = () => { if (!closed) onState("mqtt_closed_early"); };
  const ping = setInterval(() => send(0xc0), 10000);
  return {stop() { closed = true; clearInterval(ping); send(0xe0); ws.close(); }};
}

(async () => {
  "use strict";
  window.__iocLatencyProbe?.stop?.();
  const camera = "REPLACE_WITH_CAMERA_CODE", durationMs = 60000;
  const entry = [...document.querySelectorAll('script[type="module"][src]')]
    .find(s => new URL(s.src).origin === location.origin && /\/assets\/index-/.test(s.src));
  if (!entry) throw new Error("Không tìm thấy module IOC đang mở");
  // Dynamic import dùng lại module đã nạp; không tạo phiên đăng nhập/WS mới.
  const module = await import(entry.src);
  const service = Object.values(module).find(x => x && typeof x.subscribe === "function"
    && typeof x.watchCameras === "function" && typeof x.isConnected === "function");
  if (!service?.isConnected()) throw new Error("Mở camera REPLACE_WITH_CAMERA_CODE và chờ WebSocket kết nối trước");
  const videos = [...document.querySelectorAll("video")].filter(v => {
    const r = v.getBoundingClientRect(); return v.videoWidth && r.width > 50 && r.height > 50;
  });
  if (videos.length !== 1) throw new Error("Để đo đúng canvas, hãy mở riêng REPLACE_WITH_CAMERA_CODE trong một ô video");
  const video = videos[0], vr = video.getBoundingClientRect();
  const canvases = [...document.querySelectorAll("canvas")].filter(c => {
    const r = c.getBoundingClientRect();
    return r.width > 50 && r.height > 50 && r.left <= vr.left + vr.width / 2
      && r.right >= vr.left + vr.width / 2 && r.top <= vr.top + vr.height / 2
      && r.bottom >= vr.top + vr.height / 2;
  });
  // Theo dõi mọi canvas phủ video, không chỉ trường hợp trang có đúng một canvas.
  // Hook prototype có bộ lọc; không gọi getContext để tránh tạo nhầm 2D trên WebGL.
  const canvasPrototype = window.CanvasRenderingContext2D?.prototype;
  const strokeDescriptor = canvasPrototype && Object.getOwnPropertyDescriptor(canvasPrototype, "strokeRect");
  const originalStroke = canvasPrototype?.strokeRect, drawStates = new Map();
  let totalDrawCalls = 0, hookErrors = 0, anyDrawSerial = 0;
  const times = [], wsToDraw = [], wsToRaf = [], rawAge = [], videoReceiveDisplay = [];
  const wsToAnyDraw = [];
  const decode = [], videoCaptureDisplay = [], longTasks = [];
  const pairedDelta = [], pairs = new Map(), mqttStates = [];
  let mqttMessages = 0;
  const pair = (payload, channel, time) => {
    if (String(payload?.camera_code || "").toUpperCase() !== camera) return;
    let stamp = Number(payload.timestamp); if (!Number.isFinite(stamp)) return;
    if (stamp > 1e12) stamp /= 1000;
    const key = Math.round(stamp * 1000), item = pairs.get(key) || {};
    item[channel] = time;
    if (Number.isFinite(item.mqtt) && Number.isFinite(item.app)) {
      pairedDelta.push(item.app - item.mqtt); pairs.delete(key);
    } else pairs.set(key, item);
    while (pairs.size > 2048) pairs.delete(pairs.keys().next().value);
  };
  let pending = null, drawnSerial = 0, serial = 0, stopped = false, videoFrames = 0;
  let videoCallbackId = null, observer = null, hiddenEvents = document.hidden ? 1 : 0;
  const started = performance.now();
  const stats = values => {
    const s = values.filter(Number.isFinite).sort((a, b) => a - b);
    if (!s.length) return null;
    return {n: s.length, median: +s[Math.floor(s.length / 2)].toFixed(2),
      p95: +s[Math.floor(.95 * (s.length - 1))].toFixed(2), max: +s[s.length - 1].toFixed(2)};
  };
  const onVisibility = () => { if (document.hidden) hiddenEvents++; };
  document.addEventListener("visibilitychange", onVisibility);

  const drawHook = function (x, y, w, h) {
    try {
      const canvas = this.canvas, now = performance.now();
      if (!stopped && canvas?.isConnected) {
        let state = drawStates.get(canvas);
        if (!state || now - state.checked > 250) {
          const rect = canvas.getBoundingClientRect(), videoRect = video.getBoundingClientRect();
          const overlaps = rect.width > 50 && rect.height > 50
            && rect.left <= videoRect.left + videoRect.width / 2 && rect.right >= videoRect.left + videoRect.width / 2
            && rect.top <= videoRect.top + videoRect.height / 2 && rect.bottom >= videoRect.top + videoRect.height / 2;
          if (!overlaps) return originalStroke.apply(this, arguments);
          state ||= {id: drawStates.size, calls: 0, matched: 0};
          state.rect = rect; state.videoRect = videoRect; state.checked = now;
          state.width = canvas.width; state.height = canvas.height;
          state.className = String(canvas.className || "").slice(0, 120);
          drawStates.set(canvas, state);
        }
        state.calls++; totalDrawCalls++;
        if (pending && pending.serial !== anyDrawSerial) {
          wsToAnyDraw.push(now - pending.received); anyDrawSerial = pending.serial;
        }
        if (pending && pending.serial !== drawnSerial && pending.boxes.length) {
          const rect = state.rect, videoRect = state.videoRect;
          const fit = Math.min(videoRect.width / video.videoWidth, videoRect.height / video.videoHeight);
          const imageWidth = video.videoWidth * fit, imageHeight = video.videoHeight * fit;
          const left = videoRect.left + (videoRect.width - imageWidth) / 2 - rect.left;
          const top = videoRect.top + (videoRect.height - imageHeight) / 2 - rect.top;
          const dx = canvas.width / rect.width, dy = canvas.height / rect.height;
          // So trong hệ tọa độ device pixel, kể cả canvas có scale/DPR.
          const matrix = this.getTransform();
          const p1 = [matrix.a * x + matrix.c * y + matrix.e, matrix.b * x + matrix.d * y + matrix.f];
          const p2 = [matrix.a * (x + w) + matrix.c * (y + h) + matrix.e,
            matrix.b * (x + w) + matrix.d * (y + h) + matrix.f];
          const actual = [p1[0], p1[1], p2[0] - p1[0], p2[1] - p1[1]];
          for (const box of pending.boxes) {
            const normalized = box[2] <= 1 && box[3] <= 1;
            const sx = normalized ? imageWidth : fit, sy = normalized ? imageHeight : fit;
            const expected = [(left + box[0] * sx) * dx, (top + box[1] * sy) * dy,
              (box[2] - box[0]) * sx * dx, (box[3] - box[1]) * sy * dy];
            if (actual.every((v, i) => Math.abs(v - expected[i]) < 3 * Math.max(dx, dy))) {
              wsToDraw.push(now - pending.received); drawnSerial = pending.serial; state.matched++; break;
            }
          }
        }
      }
    } catch { hookErrors++; }
    return originalStroke.apply(this, arguments);
  };
  if (canvasPrototype && strokeDescriptor) Object.defineProperty(canvasPrototype, "strokeRect", {...strokeDescriptor, value: drawHook});
  const unsubscribe = service.subscribe("BBOX_UPDATE", payload => {
    if (stopped || String(payload?.camera_code || "").toUpperCase() !== camera) return;
    const now = performance.now(); times.push(now);
    pair(payload, "app", now);
    let stamp = Number(payload.timestamp);
    if (stamp > 1e12) stamp /= 1000;
    if (Number.isFinite(stamp)) rawAge.push(Date.now() - stamp * 1000);
    // Khớp rectangle mới để tránh nhầm lần redraw chỉ đổi HUD với redraw bbox.
    const boxes = (payload.detections || []).flatMap(d => [d.bbox, d.person_bbox?.bbox])
      .filter(box => Array.isArray(box) && box.length === 4 && box.every(Number.isFinite));
    pending = {serial: ++serial, received: now, boxes};
    requestAnimationFrame(() => { if (!stopped) wsToRaf.push(performance.now() - now); });
  });
  // Chỉ subscribe MQTT. Không publish bbox/event, không chạm phiên đăng nhập IOC.
  const mqttObserver = iocOpenMqttObserver(`wss://${location.host}/mqtt`, payload => {
    if (stopped) return; mqttMessages++; pair(payload, "mqtt", performance.now());
  }, state => mqttStates.push(state));
  if (video.requestVideoFrameCallback) {
    const onFrame = (now, metadata) => {
      if (stopped) return;
      videoFrames++;
      if (Number.isFinite(metadata.receiveTime)) videoReceiveDisplay.push(metadata.expectedDisplayTime - metadata.receiveTime);
      if (Number.isFinite(metadata.captureTime)) videoCaptureDisplay.push(metadata.expectedDisplayTime - metadata.captureTime);
      if (Number.isFinite(metadata.processingDuration)) decode.push(metadata.processingDuration * 1000);
      videoCallbackId = video.requestVideoFrameCallback(onFrame);
    };
    videoCallbackId = video.requestVideoFrameCallback(onFrame);
  }
  let longTaskObserverSupported = false;
  try {
    longTaskObserverSupported = PerformanceObserver.supportedEntryTypes?.includes("longtask") === true;
    observer = new PerformanceObserver(list => {
      for (const entry of list.getEntries()) if (longTasks.length < 10000) longTasks.push(entry.duration);
    });
    observer.observe({entryTypes: ["longtask"]});
  } catch { observer = null; longTaskObserverSupported = false; }
  const stop = () => {
    if (stopped) return window.__iocLatencyProbe.result;
    stopped = true; clearTimeout(timer); unsubscribe(); mqttObserver.stop(); observer?.disconnect();
    document.removeEventListener("visibilitychange", onVisibility);
    if (videoCallbackId !== null) video.cancelVideoFrameCallback?.(videoCallbackId);
    if (canvasPrototype?.strokeRect === drawHook) Object.defineProperty(canvasPrototype, "strokeRect", strokeDescriptor);
    const seconds = (performance.now() - started) / 1000;
    const result = {probe_version: 2, camera, seconds: +seconds.toFixed(2), hidden_events: hiddenEvents,
      ws_messages: times.length, ws_hz: times.length > 1 ? +((times.length - 1) * 1000 / (times.at(-1) - times[0])).toFixed(2) : 0,
      ws_interval_ms: stats(times.slice(1).map((t, i) => t - times[i])),
      mqtt_messages: mqttMessages, mqtt_states: mqttStates,
      matched_mqtt_to_app_ws_ms: stats(pairedDelta),
      ws_to_matching_canvas_command_ms: stats(wsToDraw),
      ws_to_any_canvas_command_ms: stats(wsToAnyDraw),
      canvas_candidates_at_start: canvases.length, canvas_draw_calls: totalDrawCalls, canvas_hook_errors: hookErrors,
      canvas_details: [...drawStates.values()].map(state => ({id: state.id, width: state.width, height: state.height,
        className: state.className, calls: state.calls, matched: state.matched})),
      ws_to_next_raf_ms: stats(wsToRaf), unadjusted_publish_to_browser_ms: stats(rawAge),
      video_observed_fps: +(videoFrames / seconds).toFixed(2),
      video_receive_to_display_ms: stats(videoReceiveDisplay), video_decode_ms: stats(decode),
      video_capture_to_display_ms: stats(videoCaptureDisplay), browser_long_tasks_ms: stats(longTasks),
      long_task_observer_supported: longTaskObserverSupported, browser_long_task_count: longTasks.length,
      notes: ["publish_to_browser chưa hiệu chỉnh lệch đồng hồ server/browser",
        "matched_mqtt_to_app_ws dùng cùng đồng hồ browser, gồm chênh lệch hai đường mạng; không phải riêng CPU backend",
        "canvas command không phải thời điểm pixel thật xuất hiện trên màn hình",
        "canvas chỉ đo được khi rectangle khớp; null nghĩa là chưa đo được",
        "video captureTime/receiveTime có thể không được browser cung cấp",
        "hidden_events > 0 làm sai kết quả do browser throttle tab nền"]};
    window.__iocLatencyProbe.result = result;
    console.log("IOC_LATENCY_RESULT", JSON.stringify(result, null, 2));
    return result;
  };
  const timer = setTimeout(stop, durationMs);
  window.__iocLatencyProbe = {stop, result: null};
  console.log("Đang đo 60s trên phiên IOC hiện tại. Giữ riêng REPLACE_WITH_CAMERA_CODE ở tab phía trước.");
})().catch(error => console.error("IOC latency probe:", error.message));
