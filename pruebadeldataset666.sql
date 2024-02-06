create database prueba_estambul
use prueba_estambul

CREATE TABLE compraclientes (
    invoice_no NVARCHAR(100),
    customer_id NVARCHAR(100),
    age INT,
    category NVARCHAR(100),
    quantity INT,
    price NVARCHAR(100),
    payment_method NVARCHAR(100),
    invoice_date DATE,
    shopping_mall NVARCHAR(100),
	nombre_cliente NVARCHAR(100),
	gender NVARCHAR(50)
);


select * from compraclientes

drop table compraclientes


--SELECT 
--    invoice_no,
--    COUNT(*) AS cantidad
--FROM 
--    compraclientes
--GROUP BY 
--    invoice_no
--HAVING 
--    COUNT(*) > 0; --1 si hay más de uno

------------------------------
BULK INSERT prueba_estambul.dbo.compraclientes
FROM 'D:\descargas\dataset\datasetproyectov2.csv'
WITH (
	FORMAT = 'CSV',
	FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK                  -- Ayuda a acelerar la operación de inserción masiva
)





---todo va ir en mi procedure
--CREATE PROCEDURE PoblarDimensional





--select *
--FROM compraclientes










