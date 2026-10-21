# RailReserve DCS - Distributed Railway Reservation System
**Course:** Distributed Computing Systems (DCS) - FA-2 Activity  
**Institution:** Pimpri Chinchwad College of Engineering (PCCOE), Pune  
**Department:** Information Technology | Academic Year 2026-27  

---

## Project Overview

**RailReserve DCS** is an interactive, full-stack Distributed Systems Simulation Platform designed to demonstrate, analyze, and resolve the classic **Double Booking Race Condition** in high-throughput railway reservation systems (such as IRCTC Tatkal booking surges).

The project implements a complete **Three-Tier Distributed Architecture**:
1. **Tier 1 (Clients):** Multi-device web clients (desktop browsers, smartphones, tablets) accessing the platform concurrently with real-time Server-Sent Events (SSE) synchronization.
2. **Tier 2 (Stateless Application Cluster):** Round-Robin Load Balancer dispatching requests across 3 simulated application servers (*Mumbai, Delhi, Bengaluru*) with Lamport Logical Clock ordering and Distributed Lock Management (DLM).
3. **Tier 3 (Distributed Partitioned Storage):** Horizontally partitioned regional SQLite database shards (*Western, Northern, Southern*) with asynchronous primary-replica replication.

---

## Team Members

| Name | Roll No / PRN |
| :--- | :--- |
| **Sankarshan Mahamuni** | **123B1F057** |
| **Machindra Mane** | **123B1F060** |
| **Yash Mulik** | **123B1F064** |

---

## Key DCS Concepts Implemented

- **Distributed Race Conditions:** Direct demonstration of non-atomic "Check-then-Act" design flaws.
- **Atomic Compare-and-Set (CAS):** Single-instruction atomic SQL update (`UPDATE ... WHERE status='available'`).
- **Two-Phase Commit (2PC):** Distributed transaction consensus coordinating seat allocation and payment services (Prepare -> Vote -> Global Commit).
- **Distributed Mutex (DLM):** Centralized lease coordinator with 15-second TTL auto-expiration.
- **Lamport Logical Clocks:** Monotonically increasing counter establishing total causal event ordering ($a \rightarrow b \implies C(a) < C(b)$).
- **Idempotency Keys:** Duplicate packet re-transmission suppression ensuring safe network retries without double debit.
- **Database Sharding & Replication:** Route-based horizontal partitioning with primary-replica replication.

---

## Quick Start Guide

### 1. Requirements
- Python 3.10+
- FastAPI & Uvicorn:
  ```bash
  pip install fastapi uvicorn
  ```

### 2. Run Server
Double-click `run_app.bat` or run:
```bash
python -m uvicorn server:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Open in Browser
- **On Laptop:** [http://localhost:8000](http://localhost:8000)
- **On Mobile Phone (Same Wi-Fi / Hotspot):** `http://<YOUR_LOCAL_IP>:8000` (e.g. `http://192.168.1.6:8000`)

---

## Documentation & Report
The official project report, architectural diagrams, and captured execution screenshots are located in the `docs/` folder:
- **Project Report (Word):** `docs/DCS_Mini_Project_Report_Sankarshan_Mahamuni.docx`
- **Architecture Diagrams:** `docs/diagrams/`
- **Execution Screenshots:** `docs/screenshots/`
