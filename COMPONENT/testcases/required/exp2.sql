SELECT EMPLOYEES.eid
FROM EMPLOYEES, CERTIFIED, SCHEDULE, AIRCRAFT, FLIGHTS
WHERE EMPLOYEES.eid = CERTIFIED.eid, CERTIFIED.aid = AIRCRAFT.aid, AIRCRAFT.aid = SCHEDULE.aid, SCHEDULE.flno = FLIGHTS.flno