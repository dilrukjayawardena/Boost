USE app_db;
CREATE TABLE users_tb (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100)
);

CREATE TABLE tests (
  id_key INT AUTO_INCREMENT PRIMARY KEY,
  id INT,
  name VARCHAR(100),
  address VARCHAR(200),
  color VARCHAR(50),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  is_claimed BOOLEAN,
  paid_amount DECIMAL
);

CREATE TABLE users (
  user_id VARCHAR(100) PRIMARY KEY,
  name VARCHAR(100),
  dob DATE,
  address VARCHAR(200),
  username VARCHAR(100),
  password VARBINARY(255),
  national_id VARBINARY(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE telephone_numbers (
  id_key INT AUTO_INCREMENT PRIMARY KEY,
  user_id VARCHAR(100),
  telephone_number VARCHAR(100),
  FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE jobs_history (
  id VARCHAR(100) PRIMARY KEY,
  user_id VARCHAR(100),
  occupation VARCHAR(100),
  is_fulltime BOOLEAN,
  start DATE,
  end DATE
  FOREIGN KEY (user_id) REFERENCES users(user_id)
);
