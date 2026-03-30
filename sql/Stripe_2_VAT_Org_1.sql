-- DB host: 10.10.2.42
-- DB: witisi-ordering

--select * from "Catalog" c where c."CatalogId" = '57815a0f-2be4-4247-90dc-c25b85c04746'

select  o."OrderId",
		o."Number",
		o."Email",
		o."CreatedOn",
		o."Amount",
		o."Discount",
		'(''' || o."OrderId" || ''', ''' || o."Number" || ''', ''' || o."Email" || ''', ''' || o."CreatedOn" || ''', ''' || o."Amount" || ''', ''' || o."Discount" || '''),'
from "Order" o 
join "OrderItem" oi on oi."OrderId" = o."OrderId" 
join "Catalog" c on c."CatalogId" = oi."CatalogId" 
where oi."CatalogId" = '57815a0f-2be4-4247-90dc-c25b85c04746'
	and o."State" = 'Completed'
group by o."OrderId",
		 o."Number",
		 o."Email",
		 o."CreatedOn",
		 o."Amount",
		 o."Discount"