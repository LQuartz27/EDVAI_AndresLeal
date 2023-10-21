--1
select 
  distinct category_name 
from 
  categories c;


--2
select 
  distinct region 
from 
  customers c;


--3
select 
  distinct contact_title 
from 
  customers c;


--4
select 
  * 
from 
  customers c 
order by 
  country;


--5
select 
  * 
from 
  orders o 
order by 
  employee_id;


--6
insert into customers 
values 
  (
    'EDVAI', 'EDVAI DE4 y Asociados', 
    'Andres Leal', 'Data Engineer', 
    'Cll 74N # 11-23', 'Bogota D.C', 
    'Bogota D.C', NULL, 'Colombia', '(507) 525-7777', 
    '(507) 525-7777'
  );


--7
insert into region 
values 
  (5, 'North Eastern');


--8
select 
  * 
from 
  customers 
where 
  region is null;


--9
select 
  product_name, 
  coalesce(unit_price, 10) 
from 
  products;


--10
select 
  c.company_name, 
  c.contact_name, 
  o.order_date 
from 
  orders o 
  inner join customers c on O.customer_id = C.customer_id;


--11
select 
  o.order_id, 
  p.product_name, 
  od.discount 
from 
  orders o 
  inner join order_details od on o.order_id = od.order_id 
  inner join products p on od.product_id = p.product_id;


--12
select 
  c.customer_id, 
  c.company_name, 
  o.order_id, 
  o.order_date 
from 
  customers c 
  left join orders o on c.customer_id = o.customer_id;


--13
select 
  e.employee_id, 
  e.last_name, 
  t.territory_id, 
  t.territory_description 
from 
  employees e 
  inner join employee_territories et on e.employee_id = et.employee_id 
  left join territories t on et.territory_id = t.territory_id;


--14
select 
  o.order_id, 
  c.company_name 
from 
  orders o 
  left join customers c on o.customer_id = c.customer_id;


--15
select 
  o.order_id, 
  c.company_name 
from 
  customers c 
  right join orders o on o.customer_id = c.customer_id;


--16
select 
  s.company_name, 
  o.order_date 
from 
  orders o 
  right join shippers s on o.ship_via = s.shipper_id 
where 
  --EXTRACT('Year' from order_date) = 1996
  DATE_PART('Year', order_date) = 1996;


--17
select 
  e.first_name, 
  e.last_name, 
  et.territory_id 
from 
  employees e full 
  outer join employee_territories et on e.employee_id = et.employee_id;


--18
select 
  o.order_id, 
  od.unit_price, 
  od.quantity, 
  od.unit_price * od.quantity as total 
from 
  orders o full 
  outer join order_details od on o.order_id = od.order_id;


--19
select 
  C.company_name as NAME 
from 
  customers c 
union 
select 
  S.company_name as NAME 
from 
  suppliers s;


--20
select 
  distinct first_name 
from 
  employees e;


--21
select 
  P.product_name, 
  P.product_id 
from 
  products p 
where 
  P.product_id in (
    select 
      product_id 
    from 
      order_details od
  );


--22
select 
  c.company_name 
from 
  customers c 
where 
  c.customer_id in (
    select 
      customer_id 
    from 
      orders o 
    where 
      o.ship_country = 'Argentina'
  );


--23
select 
  p.product_name 
from 
  products p 
where 
  p.product_id not in (
    select 
      od.product_id 
    from 
      order_details od 
      inner join orders o on o.order_id = od.order_id 
      inner join customers c on c.customer_id = o.customer_id 
    where 
      c.country = 'France'
  );


--24
select 
  order_id, 
  sum(quantity) 
from 
  order_details od 
group by 
  od.order_id;


--25
select 
  product_name, 
  avg(units_in_stock) 
from 
  products p 
group by 
  product_name;


--26
select 
  product_name, 
  sum(units_in_stock) 
from 
  products p 
group by 
  product_name 
having 
  sum(units_in_stock) > 100;


--27
select 
  C.company_name, 
  AVG(o.order_id) 
from 
  customers c 
  inner join orders o on O.customer_id = C.customer_id 
group by 
  C.company_name 
having 
  AVG(o.order_id) > 10;


--28
select 
  p.product_name, 
  case when p.discontinued = 1 then 'Discontinued' else c.category_name end as PRODUCT_CATEGORY 
from 
  products p 
  inner join categories c on p.category_id = c.category_id;


--29
select 
  e.first_name, 
  e.last_name, 
  case when e.title = 'Sales Manager' then 'Gerente de Ventas' else e.title end as job_title 
from 
  employees e;