-- Postgres --
CREATE TABLE "13059" (
  RevisionDPId BIGINT NOT NULL,
  Expression VARCHAR(1500) DEFAULT NULL,
  ComputeInfoJson JSONB DEFAULT NULL,
  PRIMARY KEY (RevisionDPId)
)

-- MySQL JSON --
CREATE TABLE `13059` (
  `RevisionDPId` BIGINT(20) UNSIGNED NOT NULL,
  `Expression` VARCHAR(1500) DEFAULT NULL,
  `ComputeInfoJson` JSON DEFAULT NULL,
  PRIMARY KEY (`RevisionDPId`)
)

-- MySQL Text --
CREATE TABLE `13059` (
  `RevisionDPId` BIGINT(20) UNSIGNED NOT NULL,
  `Expression` VARCHAR(1500) DEFAULT NULL,
  `ComputeInfoJson`varchar(2048),
  PRIMARY KEY (`RevisionDPId`)
)

