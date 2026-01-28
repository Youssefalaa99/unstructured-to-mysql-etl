CREATE DATABASE IF NOT EXISTS mydb;

CREATE TABLE `mydb`.`stores` (
  `store_id` integer PRIMARY KEY,
  `location` varchar(50),
  `region` ENUM('North','South','East','West')
);

CREATE TABLE `mydb`.`customers` (
  `customer_id` integer PRIMARY KEY,
  `first_name` varchar(50),
  `last_name` varchar(50),
  `email` varchar(100),
  `loyalty_member` boolean
);

CREATE TABLE `mydb`.`products` (
  `product_id` integer PRIMARY KEY,
  `sku` varchar(50),
  `category` varchar(50),
  `price` decimal(8,2)
);

CREATE TABLE `mydb`.`transactions` (
  `transaction_id` integer PRIMARY KEY,
  `created_at` timestamp,
  `store_id` integer,
  `customer_id` integer,
  `payment_method` ENUM('Credit Card','Cash','PayPal'),
  `total_amount` decimal(10,2),
  `tax` decimal(6,2)
);

CREATE TABLE `mydb`.`transaction_items` (
  `item_id` integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `transaction_id` integer,
  `product_id` integer,
  `quantity` integer
);

-- Staging
CREATE DATABASE IF NOT EXISTS staging;

CREATE TABLE `staging`.`stg_stores` (
  `store_id` integer,
  `location` varchar(50),
  `region` ENUM('North','South','East','West')
);

CREATE TABLE `staging`.`stg_customers` (
  `customer_id` integer,
  `first_name` varchar(50),
  `last_name` varchar(50),
  `email` varchar(100),
  `loyalty_member` boolean
);

CREATE TABLE `staging`.`stg_products` (
  `product_id` integer,
  `sku` varchar(50),
  `category` varchar(50),
  `price` decimal(8,2)
);


-- References

ALTER TABLE `transactions` ADD FOREIGN KEY (`store_id`) REFERENCES `stores` (`store_id`);

ALTER TABLE `transactions` ADD FOREIGN KEY (`customer_id`) REFERENCES `customers` (`customer_id`);

ALTER TABLE `transaction_items` ADD FOREIGN KEY (`transaction_id`) REFERENCES `transactions` (`transaction_id`);

ALTER TABLE `transaction_items` ADD FOREIGN KEY (`product_id`) REFERENCES `products` (`product_id`);
