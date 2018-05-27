CREATE TABLE Member (
  Username  VARCHAR(255),
  Pass      VARCHAR(255) NOT NULL,
  FirstName VARCHAR(255),
  LastName  VARCHAR(255),
  Email     VARCHAR(255),
  Status    VARCHAR(255),
  Active    BOOLEAN      NOT NULL,

  PRIMARY KEY (Username),
  CHECK (Status IN ('user', 'admin'))
);

CREATE TABLE Dataset (
  id       VARCHAR(255),
  nickname VARCHAR(255),
  metadata VARCHAR(255),
  owner    VARCHAR(255),

  FOREIGN KEY (owner) REFERENCES Member(Username) ON DELETE CASCADE,
  PRIMARY KEY (id)
);

CREATE TABLE Access (
  id_dataset VARCHAR(255),
  id_user    VARCHAR(255),
  role       VARCHAR(255),

  FOREIGN KEY (id_dataset) REFERENCES Dataset (id) ON DELETE CASCADE,
  FOREIGN KEY (id_user) REFERENCES Member (username) ON DELETE CASCADE ,
  PRIMARY KEY (id_dataset, id_user),
  CHECK (role IN ('owner', 'moderator', 'contributor'))
);

CREATE TABLE History (
  action_id   SERIAL,
  id_dataset  VARCHAR(255),
  id_table    VARCHAR(255),
  date        TIMESTAMP,
  action_desc VARCHAR(255),
  inv_query   TEXT,
  undone      BOOL,

  FOREIGN KEY (id_dataset) REFERENCES Dataset (id) ON DELETE CASCADE,
  PRIMARY KEY (action_id, id_dataset, id_table, date)
);

CREATE TABLE Metadata (
  id_dataset VARCHAR(255),
  id_table   VARCHAR(255),
  metadata   VARCHAR(255),

  FOREIGN KEY (id_dataset) REFERENCES Dataset(id) ON DELETE CASCADE,
  PRIMARY KEY (id_dataset, id_table)
);

CREATE TABLE Available_Schema (
  id INTEGER,
  PRIMARY KEY (id)
);

CREATE TABLE Active_In_Table (
  id_dataset VARCHAR(255),
  id_table   VARCHAR(255),
  id_user    VARCHAR(255),
  last_active       TIMESTAMP,

  FOREIGN KEY (id_dataset) REFERENCES Dataset(id) ON DELETE CASCADE,
  FOREIGN KEY (id_user) REFERENCES Member(Username)  ON DELETE CASCADE,
  PRIMARY KEY (id_dataset, id_user, last_active)
);

CREATE TABLE Backups (
  id_dataset VARCHAR(255),
  table_name VARCHAR(255),
  backup_name VARCHAR(255),
  timestamp TIMESTAMP,
  note VARCHAR(255),
  FOREIGN KEY (id_dataset) REFERENCES Dataset(id) ON DELETE CASCADE,
  PRIMARY KEY (id_dataset, table_name, timestamp)
);
