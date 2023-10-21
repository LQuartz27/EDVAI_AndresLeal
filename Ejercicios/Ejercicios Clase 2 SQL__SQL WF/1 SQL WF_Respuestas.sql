--1
select
	c.category_name ,
	p.product_name ,
	p.unit_price ,
	avg(p.unit_price) over(partition by c.category_name order by c.category_name) 
from
	products p
inner join categories c 
  on
	p.category_id = c.category_id;


--2
select
	avg(unit_price * quantity) over(partition by o.customer_id) as avgorderamount,
	o.order_id ,
	o.customer_id ,
	o.employee_id ,
	o.order_date ,
	o.required_date ,
	o.shipped_date 
from
	orders o
inner join order_details od 
  on o.order_id = od.order_id 
order by o.customer_id;


--3
select
	p.product_name ,
	c.category_name ,
	p.quantity_per_unit ,
	p.unit_price ,
	od.quantity ,
	avg(od.quantity) over(partition by c.category_name)
from
	products p
inner join categories c 
on
	p.category_id = c.category_id
inner join order_details od 
on
	p.product_id = od.product_id
order by 2,1;


--4
select
	c.customer_id ,
	o.order_date ,
	MIN(o.order_date) over(partition by c.customer_id) as earliest_order_date
from
	CUSTOMERS C
inner join orders o on
	C.customer_id = O.customer_id
order by customer_id ;


--5
select
	p.product_id ,
	p.product_name ,
	p.unit_price ,
	p.category_id ,
	max(p.unit_price) over (partition by p.category_id)
from
	products p
order by p.category_id ;


--6
select
	row_number() over(order by sum(od.quantity) desc),
	p.product_name ,
	sum(od.quantity)
from
	products p
inner join order_details od 
on
p.product_id = od.product_id
group by p.product_name ;


--7
select
	row_number() over(order by customer_id) as ranking,
	c.customer_id ,
	c.company_name ,
	c.contact_name ,
	c.contact_title ,
	c.address 
from
	customers c;


--8
select
	row_number() over(order by e.birth_date desc) as ranking,
	concat(e.first_name, ' ', e.last_name)  as employee_name,
	e.birth_date 
from
	employees e ;


--9
select
	sum(unit_price * quantity) over(partition by o.customer_id) as client_total_amount,
	o.order_id ,
	o.customer_id ,
	o.employee_id ,
	o.order_date ,
	o.required_date 
from
	orders o
inner join order_details od 
on o.order_id = od.order_id ;


--10
select
	c.category_name ,
	p.product_name ,
	p.unit_price ,
	od.quantity ,
	sum(od.unit_price * od.quantity) over(partition by c.category_name) as totalsales
from
	products p
inner join order_details od 
on
	p.product_id = od.product_id
inner join categories c 
on
	p.category_id = c.category_id 
order by category_name , p.product_name ;


--11
select
	c.country ,
	o.order_id ,
	o.shipped_date ,
	o.freight ,
	sum(o.freight) over(partition by c.country) as total_shipping_cost
from
	orders o
inner join customers c on
	o.customer_id = c.customer_id
where
	o.shipped_date is not null
order by 
	1,
	2
;


--12
select
	o.customer_id  ,
	c.company_name,
	sum(od.unit_price * od.quantity) as total_sales,
	rank() over(order by sum(od.unit_price * od.quantity) desc ) as rank
from
	orders o
inner join customers c on
	o.customer_id = c.customer_id
inner join order_details od on
	o.order_id = od.order_id
group by o.customer_id ,c.company_name ;


--13
select
	e.employee_id ,
	e.first_name ,
	e.last_name ,
	e.hire_date ,
	rank() over(order by e.hire_date)
from
	employees e;


--14
select
	p.product_id ,
	p.product_name ,
	p.unit_price ,
	rank() over(order by p.unit_price desc)
from
	products p;


--15
select
	od.order_id ,
	p.product_id ,
	od.quantity ,
	lag(od.quantity) over(order by order_id)
from
	products p
inner join order_details od on
p.product_id = od.product_id;


--16
select
	o.order_id ,
	o.order_date ,
	o.customer_id ,
	lag(o.order_date) over(partition by customer_id order by order_date)
from
	orders o;


--17
select
	p.product_id ,
	p.product_name ,
	p.unit_price ,
	lag(p.unit_price) over(order by p.product_id) as last_unit_price,
	p.unit_price - lag(p.unit_price) over(order by p.product_id) as price_difference
from
	products p;


--18
select
	p.product_name ,
	p.unit_price ,
	lead(p.unit_price) over(order by p.product_id) as next_unit_price
from
	products p;


--19
select
	c.category_name ,
	sum(od.quantity * od.unit_price) as total_sales ,
	lead(sum(od.quantity * od.unit_price)) over(order by c.category_name) as next_total_sales
from
	products p
inner join order_details od on
	p.product_id = od.product_id
inner join categories c on
	c.category_id = p.category_id 
group by c.category_name
;