CREATE TABLE IF NOT EXISTS `oms_order_stock_release` (
  `order_id` INT NOT NULL PRIMARY KEY,
  `session_id` INT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
