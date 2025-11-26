CREATE TABLE IF NOT EXISTS user_airports(
	email_utente VARCHAR(255),
	icao_aeroporto VARCHAR(6),
	PRIMARY KEY(email_utente, icao_aeroporto)
);

CREATE TABLE IF NOT EXISTS flight(
	id INT AUTO_INCREMENT PRIMARY KEY,
	icao_aereo VARCHAR(6) NOT NULL,
    first_seen INT,
    aeroporto_partenza VARCHAR(4) NOT NULL,
    last_seen INT,
    aeroporto_arrivo VARCHAR(4) NOT NULL,
    callsign VARCHAR(10) NULL,
    estDepartureAirportHorizDistance INT NULL,
    estDepartureAirportVertDistance INT NULL,
    estArrivalAirportHorizDistance INT NULL,
    estArrivalAirportVertDistance INT NULL,
    departureAirportCandidatesCount INT NULL,
    arrivalAirportCandidatesCount INT NULL
);