CREATE TABLE IF NOT EXISTS tasks (
    idn UUID PRIMARY KEY,
    topic VARCHAR (50) NOT NULL,
    payload JSON NOT NULL,
    status int NOT NULL,
    error VARCHAR (255) NULL,
    description VARCHAR (255) NULL
)
