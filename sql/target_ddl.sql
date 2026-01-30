CREATE DATABASE IF NOT EXISTS mydb;

DROP TABLE IF EXISTS `mydb`.`transaction_items`;
CREATE TABLE `mydb`.`transaction_items` (
  `item_id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `transaction_id` integer,
  `product_id` integer,
  `quantity` integer,
  UNIQUE KEY unique_prod_trans (transaction_id, product_id)
);

DROP TABLE IF EXISTS `mydb`.`transactions`;
CREATE TABLE `mydb`.`transactions` (
  `transaction_id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `transaction_code` varchar(20) UNIQUE,
  `created_at` datetime,
  `store_id` integer,
  `customer_id` integer,
  `payment_method` ENUM('Credit Card','Cash','PayPal'),
  `total_amount` decimal(10,2),
  `tax` decimal(6,2)
);

DROP TABLE IF EXISTS `mydb`.`stores`;
CREATE TABLE `mydb`.`stores` (
  `store_id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `store_code` varchar(20) UNIQUE,
  `location` varchar(50),
  `region` ENUM('North','South','East','West')
);

DROP TABLE IF EXISTS `mydb`.`customers`;
CREATE TABLE `mydb`.`customers` (
  `customer_id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `customer_code` varchar(20) UNIQUE,
  `first_name` varchar(50),
  `last_name` varchar(50),
  `email` varchar(100),
  `loyalty_member` boolean
);

DROP TABLE IF EXISTS `mydb`.`products`;
CREATE TABLE `mydb`.`products` (
  `product_id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `product_code` varchar(20) UNIQUE,
  `sku` varchar(50),
  `category` varchar(50),
  `price` decimal(8,2)
);

-- Staging
CREATE DATABASE IF NOT EXISTS staging;


-- References

ALTER TABLE `transactions` ADD FOREIGN KEY (`store_id`) REFERENCES `stores` (`store_id`);

ALTER TABLE `transactions` ADD FOREIGN KEY (`customer_id`) REFERENCES `customers` (`customer_id`);

ALTER TABLE `transaction_items` ADD FOREIGN KEY (`transaction_id`) REFERENCES `transactions` (`transaction_id`);

ALTER TABLE `transaction_items` ADD FOREIGN KEY (`product_id`) REFERENCES `products` (`product_id`);


-- Indexes
CREATE INDEX idx_transactions_created_at ON transactions (created_at); -- helps time-based queries (heavy but necessary)

CREATE INDEX idx_transactions_customer ON transactions (customer_id); -- speeds customer history lookups

CREATE INDEX idx_transaction_items_transaction_id ON transaction_items (transaction_id);  -- get items for this transcation

CREATE INDEX idx_customers_customer_code ON customers (customer_code); -- speeds customer code retrieval 

CREATE INDEX idx_products_product_code ON products (product_code);  -- speeds product code retreival 

CREATE INDEX idx_products_sku ON products (sku);  -- for quick sku lookups and grouping
