---------------------------------------------------
---------------------------------------------------
-- POSTGRESQL 9.1 AUTO-PARTITION TABLE BY DATE --
---------------------------------------------------
---------------------------------------------------
-- Author: https://github.com/vitovitolo

-- TRIGGER TO THAT FUNCTION

CREATE TRIGGER insert_tableA_trigger
    BEFORE INSERT ON tableA
    FOR EACH ROW EXECUTE PROCEDURE tableA_insert_function();

-- DROP TRIGGER AND FUNC

-- DROP TRIGGER insert_tableA_trigger;
-- DROP FUNCTION tableA_insert_function;

---------------------------------------------
---------------------------------------------

-- DYNAMIC FUNCTION

CREATE OR REPLACE FUNCTION tableA_insert_function()
RETURNS TRIGGER AS $$
DECLARE
    partition_name varchar;
    second_insert integer;
    create_table integer;
BEGIN

    SELECT CONCAT('tableA_'::varchar,to_char(NEW.date,'YYYYMMDD')) INTO partition_name;
    --RAISE NOTICE 'tableA_INSERT_FUNCTION FUNC: Partition name: %',partition_name;
    EXECUTE format('INSERT INTO %I VALUES ($1.*);',partition_name) USING NEW;
    RETURN NULL;
    EXCEPTION WHEN undefined_table THEN
        -- Check if partition exists
        IF NOT (SELECT exists_table(partition_name)) THEN
            -- Create new partition table with date and part name
            --RAISE NOTICE 'tableA_INSERT_FUNCTION FUNC: Creating partition table: % . With date: %',partition_name,NEW.date;
            select create_partition_table(partition_name,NEW.date) into create_table;
            IF (create_table is NULL ) THEN
                RAISE EXCEPTION 'ERROR: CLOUD NOT CREATE PARTITION %',partition_table;
            END IF;
        END IF;
        --RAISE NOTICE 'tableA_INSERT_FUNCTION FUNC: Inserting row in: %',partition_name;
        -- Insert row into partition table
        EXECUTE format('INSERT INTO %I VALUES ($1.*) RETURNING id;',partition_name) USING NEW into second_insert ;

        -- Second 'execute' statement error. The partition should be created but there are unexpected errors
        IF (second_insert is NULL ) THEN
            RAISE EXCEPTION 'ERROR: CLOUD NOT FETCH PARTITION TABLE: %.',partition_name;
        END IF;
        RETURN NULL;
END;
$$
LANGUAGE plpgsql;


------------------------

CREATE OR REPLACE FUNCTION "public"."create_partition_table"(partition_name varchar,date timestamp with time zone)
  RETURNS integer AS $$
DECLARE
        prev_table_name varchar;
        constraint_name varchar;
        min_date timestamp with time zone;
        max_date timestamp with time zone;
BEGIN

        -- Define min_date and max_date var for partition constrait
        SELECT concat(date::date,' 00:00:00+01')::timestamp with time zone into min_date;
        SELECT (date::date + interval '1 day')::timestamp with time zone into max_date;
        -- Check that new partition will be the next one in chronological order
        SELECT concat('tableA_',to_char((date::date - INTERVAL '1 day')::date,'YYYYMMDD')) INTO prev_table_name;
        --RAISE NOTICE 'CREATE_PARTITION_TABLE FUNC: Checking previous partition table: % ',prev_table_name;
        IF (SELECT exists_table(prev_table_name)) THEN
            -- Create partition table like parent table (same: defaults, indexes, storages, constraints and comments)
            EXECUTE format('CREATE TABLE %I ( LIKE tableA INCLUDING ALL ) INHERITS (tableA); ' ,partition_name);
            -- Define constraint name
            SELECT concat(partition_name,'_date_check') into constraint_name;
            -- Add partition constraint to partition table
            EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I CHECK ( date >= ''%s'' AND date < ''%s'' ) ; ' ,partition_name, constraint_name, min_date, max_date);
            -- Set permissions
            EXECUTE format('GRANT ALL PRIVILEGES ON %I TO %I;', partition_name, CURRENT_USER);

        ELSE
            RAISE EXCEPTION 'ERROR: PREVIOUS PARTITION DOES NOT EXISTS: % . PLEASE CREATE IT MANUALLY.',prev_table_name;
            RETURN 0;
        END IF;
        RETURN 1;
END;
$$  LANGUAGE 'plpgsql';

------------------------

-- Check if table name exists
CREATE OR REPLACE FUNCTION "public"."exists_table"(table_name varchar)
  RETURNS boolean AS $$
DECLARE
        output integer;
BEGIN
        EXECUTE format('SELECT count(*) FROM information_schema.tables
                        WHERE
                        table_catalog = ''%s'' AND table_schema = ''%s''
                        AND table_name = ''%s''  ', CURRENT_CATALOG::varchar,CURRENT_SCHEMA::varchar, table_name) INTO output;
        IF ( output > 0 ) THEN
            RETURN TRUE;
        ELSE
            RETURN FALSE;
        END IF;

END;
$$  LANGUAGE 'plpgsql';



--------------------------------
----------  MANUAL CREATION
--------------------------------------

CREATE TABLE tableA_20140208 ( LIKE tableA INCLUDING ALL ) INHERITS (tableA);

ALTER TABLE tableA_20140208 ADD CONSTRAINT tableA_20140208_date_check CHECK ( date >= TIMESTAMP WITH TIME ZONE '2014-02-08 00:00:00+01' AND date < TIMESTAMP WITH TIME ZONE '2014-02-09 00:00:00+01' ) ;

GRANT ALL PRIVILEGES ON tableA_20140208 TO databaseA;


--    MANUAL DELETION

DROP TABLE  tableA_20140208 ;
