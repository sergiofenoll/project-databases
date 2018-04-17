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
  id_dataset  VARCHAR(255),
  id_table    VARCHAR(255),
  date        TIMESTAMP,
  action_desc VARCHAR(255),

  FOREIGN KEY (id_dataset) REFERENCES Dataset (id) ON DELETE CASCADE,
  PRIMARY KEY (id_dataset, id_table, date)
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
