--Proceso etl

use prueba_estambul
select * FROM compraclientes
--------------------------------------------------------EJECUTALO
SELECT
invoice_no,
customer_id,
CAST(age AS int) AS 'age',
category,
CAST(quantity AS int) AS 'quantity',
CAST(price AS decimal(10,2)) AS 'price',
TRIM(payment_method) AS 'payment_method',
CONVERT(date,invoice_date,103) as 'invoice_date',
TRIM(shopping_mall) AS 'shopping_mall',
TRIM(nombre_cliente) AS 'nombre_cliente',
gender

INTO ETL_compracliente
FROM compraclientes


--añadir mas columnas necesarias al ETL_compraclientes
USE prueba_estambul; -- Asegúrate de estar en la base de datos correcta

-- Verificar si las columnas ya existen
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ETL_compracliente' AND COLUMN_NAME = 'tiempo_id')
BEGIN
    ALTER TABLE dbo.ETL_compracliente
    ADD tiempo_id INT;
END

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ETL_compracliente' AND COLUMN_NAME = 'centro_comercial_id')
BEGIN
    ALTER TABLE dbo.ETL_compracliente
    ADD centro_comercial_id INT;
END

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ETL_compracliente' AND COLUMN_NAME = 'cliente_id_key')
BEGIN
    ALTER TABLE dbo.ETL_compracliente
    ADD cliente_id_key INT;
END

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ETL_compracliente' AND COLUMN_NAME = 'payment_method_key')
BEGIN
    ALTER TABLE dbo.ETL_compracliente
    ADD payment_method_key INT;
END

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ETL_compracliente' AND COLUMN_NAME = 'categoria_id')
BEGIN
    ALTER TABLE dbo.ETL_compracliente
    ADD categoria_id INT;
END

------------------------------------------------------

select * from prueba_estambul.dbo.ETL_compracliente

select * from prueba_estambul.dbo.ETL_compracliente
-------------------------------------------------------
create database Dimensional_compraclientes
use Dimensional_compraclientes
drop database Dimensional_compraclientes
-------------------------------------------------

--creacion del modelo DIMENSIONAL V2
-- Crear Dim_Tiempo
CREATE TABLE Dim_Tiempo (
    tiempo_id INT PRIMARY KEY IDENTITY(1,1),
    factura_fecha DATE,
    dia INT,
    mes INT,
    anio INT,
    trimestre INT,
    nombre_mes VARCHAR(50)
);

-- Crear Dim_ShoppingMall
CREATE TABLE Dim_ShoppingMall (
    centro_comercial_id INT PRIMARY KEY IDENTITY(1,1),
    nombre_centro_comercial VARCHAR(50)
);

-- Crear Dim_Cliente
CREATE TABLE Dim_Cliente (
	cliente_id_key INT PRIMARY KEY IDENTITY(1,1), 
    cliente_id NVARCHAR(7),
    genero NVARCHAR(50),
    edad INT,
    nombre_cliente VARCHAR(70)
);

-- Crear Dim_MetodoPago
CREATE TABLE Dim_MetodoPago (
    payment_method_key INT PRIMARY KEY IDENTITY(1,1),
    nombre_metodo_pago VARCHAR(20),
	numero_factura NVARCHAR(7)
);

-- Crear Dim_Producto
CREATE TABLE Dim_Producto (
    categoria_id INT PRIMARY KEY IDENTITY(1,1),
    nombre_categoria VARCHAR(50)
);

-- Crear tabla de hechos (Fact_Compra)

-- Crear la tabla fact v2
CREATE TABLE fact_Compra (
    tiempo_id INT foreign key references Dim_Tiempo(tiempo_id),
    centro_comercial_id INT foreign key references Dim_ShoppingMall(centro_comercial_id),
    cliente_id_key INT foreign key references Dim_Cliente(cliente_id_key),
    payment_method_key INT foreign key references Dim_MetodoPago(payment_method_key),
    categoria_id INT foreign key references Dim_Producto(categoria_id),
    cantidad INT,
    precio DECIMAL(10, 2)
	primary key (tiempo_id,centro_comercial_id,cliente_id_key,payment_method_key,categoria_id)
);




--antes vamos a llenar las columnas de las llaves
USE prueba_estambul; -- Asegúrate de estar en la base de datos correcta
-- Insertar llaves primarias de Dim_Tiempo a ETL_compracliente
USE prueba_estambul;

CREATE OR ALTER PROCEDURE sp_InsertarLlavesETL
AS
BEGIN
    SET NOCOUNT ON;

    -- Actualizar o insertar llaves de Dim_Tiempo
    UPDATE ec
    SET
        ec.tiempo_id = t.tiempo_id
    FROM
        dbo.ETL_compracliente ec
    INNER JOIN Dimensional_compraclientes.dbo.Dim_Tiempo t ON t.factura_fecha = CONVERT(DATE, ec.invoice_date, 103);

    -- Actualizar o insertar llaves de Dim_ShoppingMall
    UPDATE ec
    SET
        ec.centro_comercial_id = s.centro_comercial_id
    FROM
        dbo.ETL_compracliente ec
    INNER JOIN Dimensional_compraclientes.dbo.Dim_ShoppingMall s ON s.nombre_centro_comercial = ec.shopping_mall;

    -- Actualizar o insertar llaves de Dim_Cliente
    UPDATE ec
    SET
        ec.cliente_id_key = c.cliente_id_key
    FROM
        dbo.ETL_compracliente ec
    INNER JOIN Dimensional_compraclientes.dbo.Dim_Cliente c ON c.cliente_id = ec.customer_id;

    -- Actualizar o insertar llaves de Dim_MetodoPago
    UPDATE ec
    SET
        ec.payment_method_key = m.payment_method_key
    FROM
        dbo.ETL_compracliente ec
    INNER JOIN Dimensional_compraclientes.dbo.Dim_MetodoPago m ON m.numero_factura = ec.invoice_no;

    -- Actualizar o insertar llaves de Dim_Producto
    UPDATE ec
    SET
        ec.categoria_id = p.categoria_id
    FROM
        dbo.ETL_compracliente ec
    INNER JOIN Dimensional_compraclientes.dbo.Dim_Producto p ON p.nombre_categoria = ec.category;

END;

--ejecuta ese procedimiento despues de llenar las tablas dimensionales
exec sp_InsertarLlavesETL





--procedimientos almacenados para cargar la base de datos dimensional
use Dimensional_compraclientes
--cargar Dim_Cliente
CREATE or alter PROCEDURE sp_cargarDim_Cliente
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.Dim_Cliente (cliente_id, genero, edad, nombre_cliente)
    SELECT 
       customer_id,
	   gender,
	   age,
	   nombre_cliente
    FROM prueba_estambul.dbo.ETL_compracliente;
END;


--cargar Dim_Producto
 CREATE PROCEDURE sp_cargarDim_Producto
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.Dim_Producto (nombre_categoria)
    SELECT 
       category
    FROM prueba_estambul.dbo.ETL_compracliente;
END;



--cargar Dim_MetodoPago
 CREATE PROCEDURE sp_cargarDim_MetodoPago
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.Dim_MetodoPago (nombre_metodo_pago,numero_factura)
    SELECT 
        payment_method,
		invoice_no
    FROM prueba_estambul.dbo.ETL_compracliente;
END;

 

 --cargar Dim_Shopping_Mall
CREATE PROCEDURE sp_cargarDim_Shopping_Mall
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.Dim_ShoppingMall (nombre_centro_comercial)
    SELECT 
        shopping_mall
    FROM prueba_estambul.dbo.ETL_compracliente;
END;



 --cargar  Dim_Tiempo
   CREATE PROCEDURE sp_cargarDim_Tiempo
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.Dim_Tiempo (factura_fecha,dia,mes,anio,trimestre,nombre_mes)
    SELECT 
        invoice_date as factura_fecha,
		DAY(invoice_date) as dia,
		MONTH(invoice_date) as mes,
		YEAR(invoice_date) as anio,
		DATEPART(quarter,invoice_date) as trimestre,
		DATENAME(MONTH,invoice_date) as nombre_mes
    FROM prueba_estambul.dbo.ETL_compracliente ;
END;

---cargar fact_Compra

CREATE or alter PROCEDURE sp_CargarFactCompra
AS
BEGIN
    INSERT INTO Dimensional_compraclientes.dbo.fact_Compra (
        tiempo_id,
        centro_comercial_id,
        cliente_id_key,
        payment_method_key,
        categoria_id,
        cantidad,
        precio
    )
    SELECT
        t.tiempo_id,
        sm.centro_comercial_id,
        c.cliente_id_key,
        mp.payment_method_key,
        p.categoria_id,
        ec.quantity,
        ec.price
    FROM prueba_estambul.dbo.ETL_compracliente ec
    JOIN Dimensional_compraclientes.dbo.Dim_Tiempo t ON ec.tiempo_id = t.tiempo_id
    JOIN Dimensional_compraclientes.dbo.Dim_ShoppingMall sm ON ec.centro_comercial_id = sm.centro_comercial_id
    JOIN Dimensional_compraclientes.dbo.Dim_Cliente c ON ec.cliente_id_key = c.cliente_id_key
    JOIN Dimensional_compraclientes.dbo.Dim_MetodoPago mp ON ec.payment_method_key = mp.payment_method_key
    JOIN Dimensional_compraclientes.dbo.Dim_Producto p ON ec.categoria_id = p.categoria_id;
END;






EXEC sp_cargarDim_Cliente
EXEC sp_cargarDim_Producto
EXEC sp_cargarDim_MetodoPago
EXEC sp_cargarDim_MetodoPago
EXEC sp_cargarDim_Shopping_Mall
EXEC sp_cargarDim_Tiempo
--antes de ejecutar el fact comprar tienes que cargar las llaves la tabla etl de prueba estambul
EXEC sp_CargarFactCompra
------------------------


use Dimensional_compraclientes
use prueba_estambul







select * from Dim_Tiempo
select * from Dim_Cliente
select * from Dim_MetodoPago
select * from Dim_Producto
select * from Dim_ShoppingMall
select * from fact_Compra








---- Insertar datos en fact_Compra
--INSERT INTO Dimensional_compraclientes.dbo.fact_Compra (
--    tiempo_id,
--    centro_comercial_id,
--    cliente_id_key,
--    payment_method_key,
--    categoria_id,
--    cantidad,
--    precio
--)
--SELECT
--    t.tiempo_id,
--    sm.centro_comercial_id,
--    c.cliente_id_key,
--    mp.payment_method_key,
--    p.categoria_id,
--    quantity,
--    price
--FROM prueba_estambul.dbo.ETL_compracliente ec
--JOIN Dimensional_compraclientes.dbo.Dim_Tiempo t ON ec.tiempo_id = t.tiempo_id
--JOIN Dimensional_compraclientes.dbo.Dim_ShoppingMall sm ON ec.centro_comercial_id = sm.centro_comercial_id
--JOIN Dimensional_compraclientes.dbo.Dim_Cliente c ON ec.cliente_id_key = c.cliente_id_key
--JOIN Dimensional_compraclientes.dbo.Dim_MetodoPago mp ON ec.payment_method_key = mp.payment_method_key
--JOIN Dimensional_compraclientes.dbo.Dim_Producto p ON ec.categoria_id = p.categoria_id;





--pivotes:

-- Supongamos que deseas obtener la cantidad de compras por categoría y método de pago para el año.
SELECT *
FROM (
    SELECT 
        D.nombre_categoria,
        T.nombre_mes,
        F.cantidad
    FROM 
        fact_Compra F
        INNER JOIN Dim_Tiempo T ON F.tiempo_id = T.tiempo_id
        INNER JOIN Dim_Producto D ON F.categoria_id = D.categoria_id
) AS SourceTable
PIVOT
(
    SUM(cantidad)
    FOR nombre_mes IN ([Enero], [Febrero], [Marzo], [Abril], [Mayo], [Junio], [Julio], [Agosto], [Septiembre], [Octubre], [Noviembre], [Diciembre])
) AS PivotTable;




			--unpivot de eseas obtener la cantidad de compras por categoría y método de pago para el año.

SELECT 
    nombre_categoria,
    nombre_mes,
    cantidad
FROM (
    SELECT 
        D.nombre_categoria,
        T.nombre_mes,
        F.cantidad
    FROM 
        fact_Compra F
        INNER JOIN Dim_Tiempo T ON F.tiempo_id = T.tiempo_id
        INNER JOIN Dim_Producto D ON F.categoria_id = D.categoria_id
) AS SourceTable
PIVOT
(
    SUM(cantidad)
    FOR nombre_mes IN ([Enero], [Febrero], [Marzo], [Abril], [Mayo], [Junio], [Julio], [Agosto], [Septiembre], [Octubre], [Noviembre], [Diciembre])
) AS PivotTable
UNPIVOT
(
    cantidad FOR nombre_mes IN ([Enero], [Febrero], [Marzo], [Abril], [Mayo], [Junio], [Julio], [Agosto], [Septiembre], [Octubre], [Noviembre], [Diciembre])
) AS UnpivotedTable;


--Pivote de Ventas por Centro Comercial y Trimestre
SELECT *
FROM (
    SELECT 
        S.nombre_centro_comercial,
        T.trimestre,
        F.cantidad
    FROM 
        fact_Compra F
        INNER JOIN Dim_Tiempo T ON F.tiempo_id = T.tiempo_id
        INNER JOIN Dim_ShoppingMall S ON F.centro_comercial_id = S.centro_comercial_id
) AS SourceTable
PIVOT
(
    SUM(cantidad)
    FOR trimestre IN ([1], [2], [3], [4])
) AS PivotTable;

		--unpivot de Ventas por Centro Comercial y Trimestre
SELECT
    nombre_centro_comercial,
    trimestre,
    cantidad
FROM (
    SELECT 
        S.nombre_centro_comercial,
        T.trimestre,
        F.cantidad
    FROM 
        fact_Compra F
        INNER JOIN Dim_Tiempo T ON F.tiempo_id = T.tiempo_id
        INNER JOIN Dim_ShoppingMall S ON F.centro_comercial_id = S.centro_comercial_id
) AS SourceTable
PIVOT
(
    SUM(cantidad)
    FOR trimestre IN ([1], [2], [3], [4])
) AS PivotTable
UNPIVOT
(
    cantidad FOR trimestre IN ([1], [2], [3], [4])
) AS UnpivotedTable;



-- Pivote de cantidad de compras por género y categoría
SELECT *
FROM (
    SELECT 
        D.nombre_categoria,
        C.genero,
        F.cantidad
    FROM 
        fact_Compra F
        INNER JOIN Dim_Producto D ON F.categoria_id = D.categoria_id
        INNER JOIN Dim_Cliente C ON F.cliente_id_key = C.cliente_id_key
) AS SourceTable
PIVOT
(
    SUM(cantidad)
    FOR genero IN ([Female], [Male],[Non-binary],[Genderfluid],[Bigender],[Polygender],[Agender],[Genderqueer])
) AS PivotTable
WHERE nombre_categoria IN ('Clothing', 'Toys', 'Shoes', 'Souvenir', 'Food & Beverage','Books','Cosmetics','Technology');

--SELECT 
--    DISTINCT genero
--FROM 
--    Dim_Cliente;


--unpivot de cantidad de compras por género y categoría
SELECT 
    nombre_categoria,
    genero,
    cantidad
FROM (
    SELECT 
        D.nombre_categoria,
        C.genero,
        F.cantidad
    FROM 
        fact_Compra F
        INNER JOIN Dim_Producto D ON F.categoria_id = D.categoria_id
        INNER JOIN Dim_Cliente C ON F.cliente_id_key = C.cliente_id_key
) AS SourceTable
PIVOT
(
    SUM(cantidad)
    FOR genero IN ([Female], [Male],[Non-binary],[Genderfluid],[Bigender],[Polygender],[Agender],[Genderqueer])
) AS PivotTable
UNPIVOT
(
    cantidad FOR genero IN ([Female], [Male],[Non-binary],[Genderfluid],[Bigender],[Polygender],[Agender],[Genderqueer])
) AS UnpivotedTable;
