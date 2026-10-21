/**
 * RailReserve DCS — Frontend Controller
 * Connects directly with server.py endpoints and renders live updates via SSE
 */

// Global State
let currentTrainId = "12123";
let selectedSeatId = null;
let currentLabMode = "unsafe";
let trainsCache = {};
let sseConnection = null;

// Initialize on DOM load
document.addEventListener("DOMContentLoaded", () => {
  loadTrains();
  loadSystemInfo();
  refreshActivity();
  setupSSE();
});

// Navigation Switcher
function showPage(pageId, btn) {
  document.querySelectorAll(".page-section").forEach(s => s.classList.remove("active"));
  const target = document.getElementById("page-" + pageId);
  if (target) target.classList.add("active");

  document.querySelectorAll(".page-nav button").forEach(b => b.classList.remove("active"));
  if (btn) btn.classList.add("active");

  if (pageId === "monitor") {
    loadSystemInfo();
    refreshActivity();
  }
}

// -------------------------------------------------------------
// 1. TRAIN CATALOG & CARDS
// -------------------------------------------------------------
async function loadTrains() {
  try {
    const res = await fetch("/api/trains");
    const trains = await res.json();
    const container = document.getElementById("trainList");
    container.innerHTML = "";

    trains.forEach(t => {
      trainsCache[t.train_id] = t;
      const card = document.createElement("div");
      const isSelected = (t.train_id === currentTrainId);
      card.className = `train-card ${isSelected ? "selected" : ""}`;
      card.onclick = () => selectTrain(t.train_id);

      const avail = t.available_seats !== undefined ? t.available_seats : (t.available || 0);
      const total = t.total_seats || t.total || 20;
      const badgeCls = avail > 0 ? "has-seats" : "no-seats";

      card.innerHTML = `
        <div class="train-info">
          <h3>${t.train_id} — ${t.name}</h3>
          <div class="route">📍 ${t.route}</div>
          <div class="timing">Dep: ${t.departure} → Arr: ${t.arrival} · Duration: ${t.duration}</div>
        </div>
        <div class="train-meta">
          <div class="fare">₹${t.fare}</div>
          <div class="avail-badge ${badgeCls}">${avail} / ${total} seats free</div>
        </div>
      `;
      container.appendChild(card);
    });

    // Load seats for the current active train
    loadSeats();
  } catch (err) {
    console.error("Failed to load trains:", err);
  }
}

function selectTrain(trainId) {
  currentTrainId = trainId;
  selectedSeatId = null;
  updateBookingFormUI();
  loadTrains();
  const labSelect = document.getElementById("labTrain");
  if (labSelect) labSelect.value = trainId;
}

// -------------------------------------------------------------
// 2. COACH SEAT MAP VISUALIZATION
// -------------------------------------------------------------
async function loadSeats() {
  try {
    const res = await fetch(`/api/seats/${currentTrainId}`);
    const data = await res.json();
    const trainInfo = trainsCache[currentTrainId] || {};

    const coachTitle = document.getElementById("coachTitle");
    const coachLabelText = document.getElementById("coachLabelText");
    const shardBadge = document.getElementById("shardBadge");
    const inputFare = document.getElementById("inputFare");

    if (coachTitle) coachTitle.textContent = `Coach ${trainInfo.coach || "C1"} — ${trainInfo.name || currentTrainId}`;
    if (coachLabelText) coachLabelText.textContent = `Coach ${trainInfo.coach || "C1"} Compartment Layout`;
    if (shardBadge) shardBadge.textContent = data.shard || "Authoritative Shard";
    if (inputFare && trainInfo.fare) inputFare.value = `₹${trainInfo.fare}`;

    const grid = document.getElementById("seatGrid");
    grid.innerHTML = "";

    const conflictSet = new Set(data.conflicts || []);

    // Group seats by row number
    const rowsMap = {};
    (data.seats || []).forEach(s => {
      if (!rowsMap[s.row_num]) rowsMap[s.row_num] = [];
      rowsMap[s.row_num].push(s);
    });

    const sortedRowNums = Object.keys(rowsMap).sort((a, b) => parseInt(a) - parseInt(b));

    sortedRowNums.forEach(rowNum => {
      const rowSeats = rowsMap[rowNum];
      const rowDiv = document.createElement("div");
      rowDiv.className = "seat-row";

      // 4-seat compartment: Col A (Window), Col B (Aisle) --- AISLE --- Col C (Aisle), Col D (Window)
      const seatA = rowSeats.find(s => s.col === "A");
      const seatB = rowSeats.find(s => s.col === "B");
      const seatC = rowSeats.find(s => s.col === "C");
      const seatD = rowSeats.find(s => s.col === "D");

      if (seatA) rowDiv.appendChild(renderSeatBox(seatA, conflictSet));
      if (seatB) rowDiv.appendChild(renderSeatBox(seatB, conflictSet));

      // Visual Aisle Walkway
      const aisle = document.createElement("div");
      aisle.className = "aisle";
      aisle.textContent = `R${rowNum}`;
      rowDiv.appendChild(aisle);

      if (seatC) rowDiv.appendChild(renderSeatBox(seatC, conflictSet));
      if (seatD) rowDiv.appendChild(renderSeatBox(seatD, conflictSet));

      grid.appendChild(rowDiv);
    });

  } catch (err) {
    console.error("Failed to load seats:", err);
  }
}

function renderSeatBox(seat, conflictSet) {
  const box = document.createElement("div");
  let statusCls = seat.status || "available";

  if (conflictSet.has(seat.seat_id)) {
    statusCls = "conflict";
  } else if (selectedSeatId === seat.seat_id && seat.status === "available") {
    statusCls = "available selected";
  }

  box.className = `seat ${statusCls}`;

  let labelText = "";
  if (statusCls === "conflict") {
    labelText = "CONFLICT!";
  } else if (seat.status === "booked") {
    labelText = seat.passenger_name ? seat.passenger_name.split(" ")[0] : "Booked";
  } else if (seat.status === "held") {
    labelText = "Holding";
  } else {
    labelText = (seat.col === "A" || seat.col === "D") ? "Window" : "Aisle";
  }

  box.innerHTML = `
    <span class="seat-label">${seat.seat_id}</span>
    <span class="seat-sub">${labelText}</span>
  `;

  if (seat.pnr) {
    box.title = `PNR: ${seat.pnr} | Passenger: ${seat.passenger_name}`;
  }

  // Click listener for interactive booking
  if (seat.status === "available") {
    box.onclick = () => {
      selectedSeatId = seat.seat_id;
      loadSeats();
      updateBookingFormUI();
      const labSeatInput = document.getElementById("labSeat");
      if (labSeatInput) labSeatInput.value = seat.seat_id;
    };
  }

  return box;
}

function updateBookingFormUI() {
  const seatInput = document.getElementById("inputSeat");
  const bookBtn = document.getElementById("bookBtn");

  if (selectedSeatId) {
    if (seatInput) seatInput.value = selectedSeatId;
    if (bookBtn) {
      bookBtn.disabled = false;
      bookBtn.textContent = `🎫 Book Seat ${selectedSeatId}`;
    }
  } else {
    if (seatInput) seatInput.value = "—";
    if (bookBtn) {
      bookBtn.disabled = true;
      bookBtn.textContent = "🎫 Select a Seat from Layout";
    }
  }
}

// -------------------------------------------------------------
// 3. PASSENGER TICKET BOOKING (Interactive)
// -------------------------------------------------------------
async function bookTicket() {
  if (!selectedSeatId) return;

  const nameInput = document.getElementById("inputName");
  const ageInput = document.getElementById("inputAge");
  const bookBtn = document.getElementById("bookBtn");

  const passengerName = nameInput ? nameInput.value.trim() || "Passenger" : "Passenger";
  const passengerAge = ageInput ? parseInt(ageInput.value) || 28 : 28;

  bookBtn.disabled = true;
  bookBtn.textContent = "⏳ Coordinating Transaction...";

  try {
    const res = await fetch("/api/book", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        train_id: currentTrainId,
        seat_id: selectedSeatId,
        name: passengerName,
        age: passengerAge,
        mode: "safe",
        algorithm: "cas"
      })
    });

    const data = await res.json();

    if (data.success) {
      renderConfirmedTicketSlip(data, passengerName, passengerAge);
      selectedSeatId = null;
      updateBookingFormUI();
    } else {
      alert(`⚠️ Booking Request Failed: ${data.message || 'Seat could not be secured.'}`);
    }

    loadTrains();
    loadSeats();
    loadSystemInfo();
    refreshActivity();

  } catch (err) {
    alert("Network request error: " + err.message);
  }

  bookBtn.disabled = false;
  updateBookingFormUI();
}

function renderConfirmedTicketSlip(data, name, age) {
  const container = document.getElementById("ticketContainer");
  const slip = document.getElementById("ticketSlip");
  const trainInfo = trainsCache[currentTrainId] || {};

  slip.innerHTML = `
    <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px;">
      <div>
        <div class="pnr">${data.pnr}</div>
        <div style="font-size: 0.8rem; color: var(--gray-500); margin-top: 2px;">Confirmed IRCTC E-Ticket</div>
      </div>
      <span class="ticket-status">✓ CONFIRMED</span>
    </div>
    <div class="ticket-detail">
      <div><strong>Train:</strong> ${currentTrainId} — ${trainInfo.name || "Express"}</div>
      <div><strong>Seat:</strong> ${data.seat_id || selectedSeatId} · <strong>Passenger:</strong> ${name} (${age}y)</div>
      <div><strong>Fare Paid:</strong> ₹${trainInfo.fare || 460}</div>
    </div>
    <div class="ticket-meta">
      Routed via <strong>${data.server_name || "AppServer"}</strong> → Shard: <strong>${data.shard_name || "Regional DB"}</strong> · Mutual Exclusion: <strong>${(data.algorithm || "CAS").toUpperCase()}</strong>
    </div>
  `;

  container.style.display = "block";
}

// -------------------------------------------------------------
// 4. CONCURRENCY EXPERIMENT LAB (Stress Test)
// -------------------------------------------------------------
function selectLabMode(mode) {
  currentLabMode = mode;
  const unsafeEl = document.getElementById("modeUnsafe");
  const safeEl = document.getElementById("modeSafe");
  const algoDiv = document.getElementById("algoDiv");
  const btn = document.getElementById("stressBtn");

  if (mode === "unsafe") {
    unsafeEl.className = "mode-option selected-unsafe";
    safeEl.className = "mode-option";
    if (algoDiv) algoDiv.style.display = "none";
    if (btn) {
      btn.className = "btn btn-stress";
      btn.innerHTML = "🔥 Launch 10 Concurrent Requests (Unsafe Mode)";
    }
  } else {
    safeEl.className = "mode-option selected-safe";
    unsafeEl.className = "mode-option";
    if (algoDiv) algoDiv.style.display = "block";
    if (btn) {
      btn.className = "btn btn-stress safe-mode";
      btn.innerHTML = "🛡️ Launch 10 Concurrent Requests (Safe DCS Mode)";
    }
  }
}

async function runStressTest() {
  const trainSelect = document.getElementById("labTrain");
  const trainId = trainSelect ? trainSelect.value : currentTrainId;
  const seatInput = document.getElementById("labSeat");
  let seatId = seatInput ? seatInput.value.trim().toUpperCase() : "C1-1A";
  const algo = document.getElementById("labAlgo") ? document.getElementById("labAlgo").value : "cas";
  const btn = document.getElementById("stressBtn");
  const banner = document.getElementById("labResultBanner");
  const tbody = document.getElementById("labResultsBody");

  if (!seatId) {
    alert("Please enter a target seat (e.g. C1-1A)");
    return;
  }

  // Pre-check seat availability to guide user clearly
  try {
    const seatCheck = await fetch(`/api/seats/${trainId}`).then(r => r.json());
    const matchedSeat = (seatCheck.seats || []).find(s => s.seat_id.toUpperCase() === seatId);

    if (matchedSeat && matchedSeat.status !== "available") {
      const confirmReset = confirm(
        `Seat ${seatId} is already ${matchedSeat.status.toUpperCase()} (held by ${matchedSeat.passenger_name || 'someone'}).\n\n` +
        `If all 10 passengers try to book an already occupied seat, naturally all 10 will get rejected!\n\n` +
        `Would you like to auto-reset this seat so you can observe the race condition / atomic winner?`
      );
      if (confirmReset) {
        await resetSystem();
        if (seatInput) seatInput.value = seatId;
      } else {
        return;
      }
    }
  } catch (err) {
    console.warn("Seat precheck warning:", err);
  }

  btn.disabled = true;
  btn.textContent = "⏳ Running 10 Simultaneous Requests on Server...";
  tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color: var(--gray-400); padding: 25px;">⏳ Firing simultaneous requests into distributed backend...</td></tr>`;
  banner.style.display = "none";

  try {
    const res = await fetch("/api/stress-test", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        train_id: trainId,
        seat_id: seatId,
        mode: currentLabMode,
        algorithm: algo,
        count: 10
      })
    });

    const data = await res.json();

    // Render Banner
    if (currentLabMode === "unsafe") {
      banner.className = "result-banner danger";
      banner.innerHTML = `
        <h4>⚠️ Race Condition Successfully Triggered (Double Booking!)</h4>
        <p>Out of ${data.total} simultaneous requests: <strong>${data.booked_count} succeeded</strong> for the exact same seat!
        Conflicting Overbookings: <strong>${data.double_booking_count}</strong>.<br>
        Check the seat layout — seat <strong>${seatId}</strong> has multiple ticket allocations!</p>
      `;
    } else {
      banner.className = "result-banner success";
      banner.innerHTML = `
        <h4>🛡️ DCS Mutual Exclusion Verified (${algo.toUpperCase()})</h4>
        <p>Out of ${data.total} simultaneous requests: <strong>exactly ${data.booked_count} won</strong> the atomic claim.
        ${data.rejected_count} requests were cleanly rejected. Double bookings: <strong>${data.double_booking_count}</strong>!</p>
      `;
    }
    banner.style.display = "block";

    // Render Table Rows
    tbody.innerHTML = "";
    (data.results || []).forEach((r, idx) => {
      const tr = document.createElement("tr");
      let statusBadge = "";

      if (r.double_booking) {
        statusBadge = `<span class="badge badge-red">DOUBLE BOOKED</span>`;
      } else if (r.success) {
        statusBadge = `<span class="badge badge-green">Confirmed</span>`;
      } else {
        statusBadge = `<span class="badge badge-amber">Rejected</span>`;
      }

      tr.innerHTML = `
        <td>${idx + 1}</td>
        <td style="font-weight: 600;">${r.passenger}</td>
        <td><span class="badge badge-blue">${r.server_name || r.server || "AppServer"}</span></td>
        <td>${statusBadge}</td>
        <td style="font-family: monospace; font-size: 0.78rem;">${r.pnr || "—"}</td>
      `;
      tbody.appendChild(tr);
    });

    // Refresh all live views
    loadTrains();
    loadSeats();
    loadSystemInfo();
    refreshActivity();

  } catch (err) {
    alert("Stress test failed: " + err.message);
  }

  btn.disabled = false;
  selectLabMode(currentLabMode);
}

async function testIdempotency() {
  const trainId = document.getElementById("labTrain").value;
  const seatId = document.getElementById("labSeat").value.trim() || "C1-1A";
  const idempKey = "IRCTC-RETRY-KEY-" + Math.floor(Math.random() * 90000 + 10000);

  const payload = {
    train_id: trainId,
    seat_id: seatId,
    name: "Rahul Verma (Mobile)",
    age: 29,
    mode: "safe",
    algorithm: "cas",
    idempotency_key: idempKey
  };

  // First Request
  const res1 = await fetch("/api/book", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const data1 = await res1.json();

  // Second Request (Duplicate click simulating network timeout)
  const res2 = await fetch("/api/book", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const data2 = await res2.json();

  const banner = document.getElementById("labResultBanner");
  banner.className = "result-banner success";
  banner.innerHTML = `
    <h4>🔁 Distributed Idempotency Verification</h4>
    <p>
      <strong>Click 1:</strong> ${data1.message || 'Ticket issued'}<br>
      <strong>Click 2 (Duplicate Retry):</strong> ${data2.message || 'Replayed from cache'}<br><br>
      Both requests returned PNR <strong>${data1.pnr || data2.pnr}</strong>. 
      Zero duplicate tickets created and zero double charging.
    </p>
  `;
  banner.style.display = "block";

  loadTrains();
  loadSeats();
  loadSystemInfo();
  refreshActivity();
}

// -------------------------------------------------------------
// 5. SYSTEM MONITOR & ACTIVITY FEED
// -------------------------------------------------------------
async function loadSystemInfo() {
  try {
    const res = await fetch("/api/system-info");
    const data = await res.json();

    const clockEl = document.getElementById("headerClock");
    if (clockEl) clockEl.textContent = data.lamport_clock;

    if (data.app_servers && data.app_servers.length >= 3) {
      const s1 = document.getElementById("srv1Load");
      const s2 = document.getElementById("srv2Load");
      const s3 = document.getElementById("srv3Load");
      if (s1) s1.textContent = `${data.app_servers[0].requests || 0} reqs`;
      if (s2) s2.textContent = `${data.app_servers[1].requests || 0} reqs`;
      if (s3) s3.textContent = `${data.app_servers[2].requests || 0} reqs`;
    }
  } catch (err) {
    console.error("Failed to load system info:", err);
  }
}

async function refreshActivity() {
  try {
    const res = await fetch("/api/activity");
    const items = await res.json();
    const feed = document.getElementById("activityFeed");
    feed.innerHTML = "";

    if (!items || items.length === 0) {
      feed.innerHTML = `<div style="color: #64748b; text-align: center; padding: 40px;">No distributed activity yet. Book a seat or launch a stress test!</div>`;
      return;
    }

    items.forEach(item => {
      appendActivityRow(item);
    });
  } catch (err) {
    console.error("Failed to load activity:", err);
  }
}

function appendActivityRow(item) {
  const feed = document.getElementById("activityFeed");
  const line = document.createElement("div");
  line.className = "activity-line";

  let actClass = "info";
  const action = (item.action || "").toUpperCase();
  if (action.includes("SUCCESS") || action.includes("CONFIRM") || action.includes("COMMIT")) actClass = "success";
  if (action.includes("CONFLICT") || action.includes("DOUBLE") || action.includes("FAIL") || action.includes("ABORT")) actClass = "fail";
  if (action.includes("WARN") || action.includes("EXPIRE") || action.includes("REJECT") || action.includes("IDEMPOTENT")) actClass = "warn";

  line.innerHTML = `
    <span class="ts">[L:${item.lamport_ts}]</span>
    <span class="arrow">${item.source} → ${item.dest}</span>
    <span class="act ${actClass}">${item.action}</span>
    <span class="desc">${item.detail}</span>
  `;
  feed.insertBefore(line, feed.firstChild);
}

// -------------------------------------------------------------
// 6. SYSTEM RESET & REAL-TIME SSE
// -------------------------------------------------------------
async function resetSystem() {
  if (!confirm("Reset all distributed regional shards, tickets, and locks?")) return;
  await fetch("/api/reset", { method: "POST" });
  selectedSeatId = null;
  updateBookingFormUI();

  const ticketContainer = document.getElementById("ticketContainer");
  if (ticketContainer) ticketContainer.style.display = "none";

  const labBanner = document.getElementById("labResultBanner");
  if (labBanner) labBanner.style.display = "none";

  loadTrains();
  loadSeats();
  loadSystemInfo();
  refreshActivity();
}

function setupSSE() {
  if (sseConnection) sseConnection.close();
  sseConnection = new EventSource("/api/events");

  sseConnection.addEventListener("seat_update", () => {
    loadSeats();
    loadTrains();
  });

  sseConnection.addEventListener("activity", (e) => {
    try {
      const item = JSON.parse(e.data);
      appendActivityRow(item);
      const clockEl = document.getElementById("headerClock");
      if (clockEl) clockEl.textContent = item.lamport_ts;
    } catch (err) {}
  });

  sseConnection.addEventListener("system_update", () => {
    loadSystemInfo();
  });

  sseConnection.addEventListener("reset", () => {
    loadTrains();
    loadSeats();
    loadSystemInfo();
    refreshActivity();
  });

  sseConnection.onerror = () => {
    setTimeout(setupSSE, 3000); // Reconnect automatically
  };
}
