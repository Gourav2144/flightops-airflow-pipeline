// ============================================================
// FlightOps MongoDB Init Script
// Seeds 5 collections with realistic aviation data
// Simulates: DocumentDB (AWS) with business keys
// ============================================================

db = db.getSiblingDB('flight_data');

// ── FlightSchedules ──────────────────────────────────────
db.FlightSchedules.drop();
db.FlightSchedules.insertMany([
  { flightScheduleId: NumberLong(1001), BusinessKey: "UA::100::2025-06-01T08:00:00", flightNumber: "UA100", origin: "ORD", destination: "LAX", departureTime: new Date("2025-06-01T08:00:00Z"), status: "Scheduled", aircraftType: "B737", createdDtm: new Date() },
  { flightScheduleId: NumberLong(1002), BusinessKey: "UA::101::2025-06-01T10:30:00", flightNumber: "UA101", origin: "LAX", destination: "JFK", departureTime: new Date("2025-06-01T10:30:00Z"), status: "Scheduled", aircraftType: "B757", createdDtm: new Date() },
  { flightScheduleId: NumberLong(1003), BusinessKey: "UA::200::2025-06-02T06:00:00", flightNumber: "UA200", origin: "JFK", destination: "ORD", departureTime: new Date("2025-06-02T06:00:00Z"), status: "Scheduled", aircraftType: "A320", createdDtm: new Date() },
  { flightScheduleId: NumberLong(1004), BusinessKey: "UA::201::2025-06-02T14:15:00", flightNumber: "UA201", origin: "ORD", destination: "DEN", departureTime: new Date("2025-06-02T14:15:00Z"), status: "Scheduled", aircraftType: "B737", createdDtm: new Date() },
  { flightScheduleId: NumberLong(1005), BusinessKey: "UA::300::2025-06-03T09:45:00", flightNumber: "UA300", origin: "DEN", destination: "SFO", departureTime: new Date("2025-06-03T09:45:00Z"), status: "Scheduled", aircraftType: "B757", createdDtm: new Date() },
  { flightScheduleId: NumberLong(1006), BusinessKey: "UA::301::2025-06-03T16:00:00", flightNumber: "UA301", origin: "SFO", destination: "SEA", departureTime: new Date("2025-06-03T16:00:00Z"), status: "Scheduled", aircraftType: "A320", createdDtm: new Date() },
  { flightScheduleId: NumberLong(1007), BusinessKey: "UA::400::2025-06-04T07:30:00", flightNumber: "UA400", origin: "SEA", destination: "LAS", departureTime: new Date("2025-06-04T07:30:00Z"), status: "Scheduled", aircraftType: "B737", createdDtm: new Date() },
  { flightScheduleId: NumberLong(1008), BusinessKey: "UA::401::2025-06-04T13:00:00", flightNumber: "UA401", origin: "LAS", destination: "PHX", departureTime: new Date("2025-06-04T13:00:00Z"), status: "Scheduled", aircraftType: "B757", createdDtm: new Date() },
]);
db.FlightSchedules.createIndex({ flightScheduleId: 1 });
db.FlightSchedules.createIndex({ BusinessKey: 1 }, { unique: true });
print("FlightSchedules: seeded " + db.FlightSchedules.countDocuments() + " records");

// ── CrewAssignments ──────────────────────────────────────
db.CrewAssignments.drop();
db.CrewAssignments.insertMany([
  { crewAssignmentId: NumberLong(2001), BusinessKey: "EMP001::UA100::2025-06-01", employeeId: "EMP001", flightNumber: "UA100", role: "Captain", fromDtm: new Date("2025-06-01T08:00:00Z"), toDtm: new Date("2025-06-01T11:30:00Z"), status: "Confirmed", createdDtm: new Date() },
  { crewAssignmentId: NumberLong(2002), BusinessKey: "EMP002::UA100::2025-06-01", employeeId: "EMP002", flightNumber: "UA100", role: "FirstOfficer", fromDtm: new Date("2025-06-01T08:00:00Z"), toDtm: new Date("2025-06-01T11:30:00Z"), status: "Confirmed", createdDtm: new Date() },
  { crewAssignmentId: NumberLong(2003), BusinessKey: "EMP003::UA101::2025-06-01", employeeId: "EMP003", flightNumber: "UA101", role: "Captain", fromDtm: new Date("2025-06-01T10:30:00Z"), toDtm: new Date("2025-06-01T19:00:00Z"), status: "Confirmed", createdDtm: new Date() },
  { crewAssignmentId: NumberLong(2004), BusinessKey: "EMP004::UA200::2025-06-02", employeeId: "EMP004", flightNumber: "UA200", role: "Captain", fromDtm: new Date("2025-06-02T06:00:00Z"), toDtm: new Date("2025-06-02T09:00:00Z"), status: "Confirmed", createdDtm: new Date() },
  { crewAssignmentId: NumberLong(2005), BusinessKey: "EMP005::UA201::2025-06-02", employeeId: "EMP005", flightNumber: "UA201", role: "FirstOfficer", fromDtm: new Date("2025-06-02T14:15:00Z"), toDtm: new Date("2025-06-02T16:45:00Z"), status: "Confirmed", createdDtm: new Date() },
  { crewAssignmentId: NumberLong(2006), BusinessKey: "EMP006::UA300::2025-06-03", employeeId: "EMP006", flightNumber: "UA300", role: "Captain", fromDtm: new Date("2025-06-03T09:45:00Z"), toDtm: new Date("2025-06-03T12:15:00Z"), status: "Confirmed", createdDtm: new Date() },
]);
db.CrewAssignments.createIndex({ crewAssignmentId: 1 });
db.CrewAssignments.createIndex({ BusinessKey: 1 }, { unique: true });
print("CrewAssignments: seeded " + db.CrewAssignments.countDocuments() + " records");

// ── RouteOperations ──────────────────────────────────────
db.RouteOperations.drop();
db.RouteOperations.insertMany([
  { routeOperationId: NumberLong(3001), BusinessKey: "ORD-LAX::2025-06-01", routeCode: "ORD-LAX", distance: 1745, avgFlightMins: 210, activeFlights: 12, operationalStatus: "Active", bidPeriod: 202506, createdDtm: new Date() },
  { routeOperationId: NumberLong(3002), BusinessKey: "LAX-JFK::2025-06-01", routeCode: "LAX-JFK", distance: 2475, avgFlightMins: 300, activeFlights: 8, operationalStatus: "Active", bidPeriod: 202506, createdDtm: new Date() },
  { routeOperationId: NumberLong(3003), BusinessKey: "JFK-ORD::2025-06-02", routeCode: "JFK-ORD", distance: 1190, avgFlightMins: 165, activeFlights: 15, operationalStatus: "Active", bidPeriod: 202506, createdDtm: new Date() },
  { routeOperationId: NumberLong(3004), BusinessKey: "ORD-DEN::2025-06-02", routeCode: "ORD-DEN", distance: 920, avgFlightMins: 150, activeFlights: 10, operationalStatus: "Active", bidPeriod: 202506, createdDtm: new Date() },
  { routeOperationId: NumberLong(3005), BusinessKey: "DEN-SFO::2025-06-03", routeCode: "DEN-SFO", distance: 1250, avgFlightMins: 175, activeFlights: 9, operationalStatus: "Active", bidPeriod: 202506, createdDtm: new Date() },
]);
db.RouteOperations.createIndex({ routeOperationId: 1 });
db.RouteOperations.createIndex({ BusinessKey: 1 }, { unique: true });
print("RouteOperations: seeded " + db.RouteOperations.countDocuments() + " records");

// ── AircraftStatus ───────────────────────────────────────
db.AircraftStatus.drop();
db.AircraftStatus.insertMany([
  { aircraftStatusId: NumberLong(4001), BusinessKey: "N12345::2025-06-01T00:00:00", tailNumber: "N12345", aircraftType: "B737", homeBase: "ORD", statusCode: "ACTIVE", maintenanceDue: new Date("2025-08-01T00:00:00Z"), totalFlightHours: 12500, createdDtm: new Date() },
  { aircraftStatusId: NumberLong(4002), BusinessKey: "N23456::2025-06-01T00:00:00", tailNumber: "N23456", aircraftType: "B757", homeBase: "LAX", statusCode: "ACTIVE", maintenanceDue: new Date("2025-07-15T00:00:00Z"), totalFlightHours: 18200, createdDtm: new Date() },
  { aircraftStatusId: NumberLong(4003), BusinessKey: "N34567::2025-06-01T00:00:00", tailNumber: "N34567", aircraftType: "A320", homeBase: "JFK", statusCode: "MAINTENANCE", maintenanceDue: new Date("2025-06-10T00:00:00Z"), totalFlightHours: 9800, createdDtm: new Date() },
  { aircraftStatusId: NumberLong(4004), BusinessKey: "N45678::2025-06-02T00:00:00", tailNumber: "N45678", aircraftType: "B737", homeBase: "DEN", statusCode: "ACTIVE", maintenanceDue: new Date("2025-09-01T00:00:00Z"), totalFlightHours: 7200, createdDtm: new Date() },
  { aircraftStatusId: NumberLong(4005), BusinessKey: "N56789::2025-06-02T00:00:00", tailNumber: "N56789", aircraftType: "B757", homeBase: "SFO", statusCode: "ACTIVE", maintenanceDue: new Date("2025-10-15T00:00:00Z"), totalFlightHours: 22100, createdDtm: new Date() },
]);
db.AircraftStatus.createIndex({ aircraftStatusId: 1 });
db.AircraftStatus.createIndex({ BusinessKey: 1 }, { unique: true });
print("AircraftStatus: seeded " + db.AircraftStatus.countDocuments() + " records");

// ── PassengerBookings ────────────────────────────────────
db.PassengerBookings.drop();
db.PassengerBookings.insertMany([
  { passengerBookingId: NumberLong(5001), BusinessKey: "PNR001::UA100::2025-06-01", pnr: "PNR001", flightNumber: "UA100", passengerCount: 142, bookingClass: "Y", fareAmount: 28400.00, bookingDtm: new Date("2025-05-15T00:00:00Z"), status: "Confirmed", createdDtm: new Date() },
  { passengerBookingId: NumberLong(5002), BusinessKey: "PNR002::UA101::2025-06-01", pnr: "PNR002", flightNumber: "UA101", passengerCount: 198, bookingClass: "Y", fareAmount: 51480.00, bookingDtm: new Date("2025-05-16T00:00:00Z"), status: "Confirmed", createdDtm: new Date() },
  { passengerBookingId: NumberLong(5003), BusinessKey: "PNR003::UA200::2025-06-02", pnr: "PNR003", flightNumber: "UA200", passengerCount: 156, bookingClass: "B", fareAmount: 46800.00, bookingDtm: new Date("2025-05-17T00:00:00Z"), status: "Confirmed", createdDtm: new Date() },
  { passengerBookingId: NumberLong(5004), BusinessKey: "PNR004::UA201::2025-06-02", pnr: "PNR004", flightNumber: "UA201", passengerCount: 89,  bookingClass: "Y", fareAmount: 17800.00, bookingDtm: new Date("2025-05-18T00:00:00Z"), status: "Waitlisted", createdDtm: new Date() },
  { passengerBookingId: NumberLong(5005), BusinessKey: "PNR005::UA300::2025-06-03", pnr: "PNR005", flightNumber: "UA300", passengerCount: 203, bookingClass: "F", fareAmount: 91350.00, bookingDtm: new Date("2025-05-19T00:00:00Z"), status: "Confirmed", createdDtm: new Date() },
]);
db.PassengerBookings.createIndex({ passengerBookingId: 1 });
db.PassengerBookings.createIndex({ BusinessKey: 1 }, { unique: true });
print("PassengerBookings: seeded " + db.PassengerBookings.countDocuments() + " records");

print("MongoDB init complete.");
