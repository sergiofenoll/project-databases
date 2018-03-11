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
)