BEGIN;

CREATE OR REPLACE FUNCTION tcloud_fill_canonical_id_from_id()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.canonical_id IS NULL THEN
        NEW.canonical_id := NEW.id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_tcloud_folders_canonical_id
ON telegram_index_folders;

CREATE TRIGGER trg_tcloud_folders_canonical_id
BEFORE INSERT OR UPDATE
ON telegram_index_folders
FOR EACH ROW
EXECUTE FUNCTION tcloud_fill_canonical_id_from_id();

DROP TRIGGER IF EXISTS trg_tcloud_files_canonical_id
ON telegram_index_files;

CREATE TRIGGER trg_tcloud_files_canonical_id
BEFORE INSERT OR UPDATE
ON telegram_index_files
FOR EACH ROW
EXECUTE FUNCTION tcloud_fill_canonical_id_from_id();

UPDATE telegram_index_folders
SET canonical_id = id
WHERE canonical_id IS NULL;

UPDATE telegram_index_files
SET canonical_id = id
WHERE canonical_id IS NULL;

COMMIT;