-- Создаём схемы, если их ещё нет
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'raw')
    EXEC('CREATE SCHEMA raw');

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dwh')
    EXEC('CREATE SCHEMA dwh');
