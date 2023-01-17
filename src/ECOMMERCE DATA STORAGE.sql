drop database ecommerce_record;
create database ecommerce_record;

use ecommerce_record;

create table IF NOT EXISTS users (
	user_id varchar(10) primary key,
	user_name varchar(45) not null,
	user_email varchar(45) not null,
	user_password varchar(45) not null,
	user_address varchar(45) not null,
	is_vendor tinyint(1)
    );
    
create table IF NOT EXISTS products (
	product_id varchar(45) not null primary key,
	product_name varchar(45) not null,
	product_price double not null,
	product_description varchar(100) not null,
	vendor_id varchar(10) not null,
	constraint vendor_id foreign key(vendor_id) references users(user_id),
	emi_available varchar(10) not null);
    
create table IF NOT EXISTS orders (
	order_id int not null primary key,
	customer_id varchar(10) not null,
	vendor_id varchar(10) not null,
	total_value float(45) not null,
	order_quantity int not null,
	reward_point int not null,
	constraint vendor_id_ibfk_1 foreign key(vendor_id) references users(user_id),
	constraint customer_id foreign key(customer_id) references users(user_id));
    
create table IF NOT EXISTS customer_leaderboard (
	customer_id varchar(10) not null primary key,
	customer_name varchar(45) not null,
	customer_email varchar(45) not null,
	total_value float(45) not null,
	constraint fk_customer_id foreign key(customer_id) references users(user_id));
    
show tables;
desc users;
desc products;
desc orders;
desc customer_leaderboard;


select * from users;
select * from products;
select * from orders;
select * from customer_leaderboard;

INSERT INTO users VALUES 
(1,'vendor_1','vendor_1@example.com','default_vendor1',' ', 1),
(2,'vendor_2','vendor_2@example.com','default_vendor2',' ', 1),
(3,'vendor_3','vendor_3@example.com','default_vendor3',' ', 1),
(4,'vendor_4','vendor_4@example.com','default_vendor4',' ', 1),
(5,'vendor_5','vendor_5@example.com','default_vendor5',' ', 1),
(6,'admin','admin4@example.com','admin','790,Kozey Meadow Apt', 0),
(7,'user_1','user_1@example.com','default_user1','8589,Miller Centers Leannonmouth',0),
(8,'user_2','user_2@example.com','default_user2','958, Gerry Estate New Eudora',0),
(9,'user_3','user_3@example.com','default_user3','Primary School Frontgate, Gotham', 0),
(10,'user_4','user_4@example.com','default_user4','City Hospital, Gotham', 0),
(11,'user_5','user_5@example.com','default_user5','15, Yemen Road, Yemen', 0),
(12,'user_6','user_6@example.com','default_user6','Dunder mifflin, Scranton', 0),
(13,'user_7','user_7@example.com','default_user7','Primary School Frontgate Gotham', 0),
(14,'user8_','user_8@example.com','default_user8','99th Precinct, Brooklyn', 0),
(15,'user_9','user_9@example.com','default_user9','Head office GreatLearning, Girgaon', 0);

INSERT INTO products VALUES 
('P01','iphone 12',64000,'Mobile',1,Yes),
('P02','ipad pro',81000,'Tablet',3,Yes),
('P03','macbook air',75000,'Laptop',4,Yes),
('P04','Sony 43 inch',55000,'TV',2,Yes),
('P05','Samsung M31S',22000,'Mobile',5,No),
('P06','Samsung A01',9000,'Mobile',5,No),
('P07','Samsung 43 inch',45000,'TV',4,Yes),
('P08','logi Keyboard',3500,'Accessories',2,No),
('P09','Apple Pencil',8500,'Accessories',3,No),
('P10','iphone 13',79000,'Mobile',1,Yes);

INSERT INTO orders VALUES
(1,11,5,36480,2,200),
(2,9,5,73291,5,100),
(3,7,5,89414,3,100),
(4,10,4,42084,3,100),
(5,7,5,43547,1,100),
(6,6,5,82280,4,200),
(7,7,2,127616,2,200),
(8,11,2,138303,3,200),
(9,11,1,88289,5,100),
(10,7,2,99797,1,100),
(11,7,5,112859,2,300),
(12,9,5,28498,2,100),
(13,13,3,87087,5,100),
(14,8,5,106852,4,200),
(15,8,4,135512,5,200),
(16,12,1,50195,5,200),
(17,13,2,120845,2,300),
(18,8,2,128129,4,300),
(19,8,4,116116,1,100),
(20,6,1,19883,1,100),
(21,6,2,40848,5,300),
(22,6,1,142904,2,300),
(23,13,4,22420,4,100),
(24,11,4,103695,2,300),
(25,12,2,62479,4,200),
(26,10,2,145789,4,100),
(27,10,2,33659,1,300),
(28,13,5,90516,4,100),
(29,7,4,132557,4,200),
(30,13,5,18329,1,200),
(31,11,2,99585,4,100),
(32,12,5,92354,1,100),
(33,8,4,133375,2,100),
(34,11,2,36368,4,200),
(35,12,1,21647,4,300),
(36,7,5,148461,3,100),
(37,12,5,116147,3,100),
(38,9,5,77404,3,300),
(39,7,3,121081,3,200),
(40,9,2,128282,2,200),
(41,10,5,14374,3,300),
(42,8,4,18957,4,100),
(43,11,5,60403,4,100),
(44,13,3,112006,4,100),
(45,13,3,123444,3,200),
(46,7,3,24638,4,300),
(47,8,4,132860,5,100),
(48,13,4,116806,1,300),
(49,9,2,127253,5,200),
(50,8,3,81953,2,300),
(51,8,3,19759,2,100),
(52,9,5,124219,3,200),
(53,8,3,135965,3,200),
(54,7,1,81634,2,300),
(55,10,1,95367,1,300),
(56,10,4,104508,1,300),
(57,13,4,29532,3,200),
(58,9,2,89946,1,300),
(59,10,2,57468,4,200),
(60,6,1,42662,2,100),
(61,10,4,67616,2,200),
(62,10,1,72174,2,200),
(63,13,1,75784,2,100),
(64,7,5,96575,1,100),
(65,7,1,102407,5,200),
(66,7,3,15729,3,200),
(67,12,2,31469,3,100),
(68,10,4,114344,4,100),
(69,9,2,119425,2,100),
(70,12,3,46420,2,200),
(71,13,1,148881,2,100),
(72,13,4,136206,1,300),
(73,12,3,26697,2,100),
(74,10,3,106621,2,100),
(75,7,5,81423,3,300),
(76,9,2,109141,4,200),
(77,10,4,143279,4,300),
(78,10,5,130024,1,200),
(79,8,5,40882,4,300),
(80,8,1,79404,4,300),
(81,13,4,139303,1,100),
(82,12,2,94868,3,100),
(83,7,2,53399,2,100),
(84,11,1,20389,4,100),
(85,8,5,37855,1,300),
(86,7,5,82617,4,300),
(87,6,2,12264,4,300),
(88,13,2,99007,1,300),
(89,7,1,65032,4,300),
(90,10,3,140251,3,100),
(91,12,2,110957,2,300),
(92,6,2,14351,2,200),
(93,13,2,131439,1,100),
(94,13,3,14752,4,100),
(95,11,3,139844,1,100),
(96,6,4,93647,1,200),
(97,9,1,144686,4,100),
(98,8,2,71421,5,300),
(99,11,1,15062,5,100),
(100,13,4,16806,5,300);

select o.customer_id,c.user_name, c.user_email, max(o.total_value) as max_value as customer_leaderboard
from ecommerce_record.orders o
left join ecommerce_record.users c on o.customer_id = c.user_id
group by o.customer_id

INSERT INTO orders VALUES
(101,12895,2,300,'3','15'),
(102,25413,4,100,'5','13'),
(103,42156,7,30,'2','14'),
(104,10263,6,200,'1','5'),
(105,7895,2,0,'4','7');

select * from orders;

select * from orders
where total_value = (select min(total_value) from orders);

select * from orders
where total_value = (select max(total_value) from orders);

select * from orders
where total_value> (select avg(total_value) from orders);



    
