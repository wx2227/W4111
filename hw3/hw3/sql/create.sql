CREATE SCHEMA `CSVCatalog` ;

CREATE TABLE `CSVCatalog`.`csvtables` (
  `table_name` VARCHAR(20) NOT NULL,
  `file_name` VARCHAR(45) NULL,
  PRIMARY KEY (`table_name`));

CREATE TABLE `CSVCatalog`.`csvtablecolumns` (
  `table_name` VARCHAR(20) NOT NULL,
  `column_name` VARCHAR(20) NOT NULL,
  `type` ENUM('text', 'number') NULL,
  `not_null` TINYINT NULL,
  PRIMARY KEY (`table_name`, `column_name`));

CREATE TABLE `CSVCatalog`.`csvtableindexes` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `table_name` VARCHAR(20) NOT NULL,
  `index_name` VARCHAR(20) NOT NULL,
  `kind` ENUM('PRIMARY', 'INDEX', 'UNIQUE') NOT NULL,
  `column_name` VARCHAR(20) NOT NULL,
  `key_column_order` INT NOT NULL,
  PRIMARY KEY (`id`));
