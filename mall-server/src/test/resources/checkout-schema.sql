CREATE TABLE pms_product (id INT PRIMARY KEY, stock INT NOT NULL);

CREATE TABLE oms_order (
    id INT PRIMARY KEY,
    user_id INT NOT NULL,
    seckill_session_id INT,
    address_id INT,
    order_sn VARCHAR(64),
    payment_sn VARCHAR(64),
    total_amount DECIMAL(10,2),
    pay_method INT,
    express_delivery INT,
    status INT NOT NULL,
    recipient_name VARCHAR(128),
    recipient_phone VARCHAR(32),
    recipient_address VARCHAR(255),
    payment_time TIMESTAMP,
    delivery_time TIMESTAMP,
    receipt_time TIMESTAMP,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_deleted INT DEFAULT 0,
    UNIQUE (user_id, seckill_session_id)
);

CREATE TABLE oms_order_item (
    id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    product_name VARCHAR(128),
    product_price DECIMAL(10,2),
    product_img VARCHAR(128),
    amount INT NOT NULL,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_deleted INT DEFAULT 0
);

CREATE TABLE oms_order_stock_release (
    order_id INT PRIMARY KEY,
    session_id INT NOT NULL
);
