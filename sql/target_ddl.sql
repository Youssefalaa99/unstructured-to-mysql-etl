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
  `transaction_code` varchar(10),
  `created_at` timestamp,
  `store_id` integer,
  `customer_id` integer,
  `payment_method` ENUM('Credit Card','Cash','PayPal'),
  `total_amount` decimal(10,2),
  `tax` decimal(6,2)
);

DROP TABLE IF EXISTS `mydb`.`stores`;
CREATE TABLE `mydb`.`stores` (
  `store_id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `store_code` varchar(10),
  `location` varchar(50),
  `region` ENUM('North','South','East','West')
);

DROP TABLE IF EXISTS `mydb`.`customers`;
CREATE TABLE `mydb`.`customers` (
  `customer_id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `customer_code` varchar(10),
  `first_name` varchar(50),
  `last_name` varchar(50),
  `email` varchar(100),
  `loyalty_member` boolean
);

DROP TABLE IF EXISTS `mydb`.`products`;
CREATE TABLE `mydb`.`products` (
  `product_id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `product_code` varchar(10),
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
