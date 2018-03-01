CREATE TABLE Member (
  Username VARCHAR(255),
  Pass VARCHAR(255) NOT NULL,
  FirstName VARCHAR(255),
  LastName VARCHAR(255),
  Email VARCHAR(255),
  Status VARCHAR(255),
  Active BOOLEAN NOT NULL,
  
  PRIMARY KEY(Username),
  CHECK (Status IN ('user','admin'))
);

CREATE TABLE Dataset (
  id VARCHAR(255),
  nickname VARCHAR(255),
  metadata VARCHAR(255),
  PRIMARY KEY(id)
);

CREATE TABLE Access (
  id_dataset VARCHAR(255),
  id_user VARCHAR(255),
  role VARCHAR(255),
  FOREIGN KEY(id_dataset) REFERENCES Dataset(id),
  FOREIGN KEY(id_user) REFERENCES Member(username),
  PRIMARY KEY(id_dataset, id_user),
  CHECK (role IN ('owner','contributer'))
);