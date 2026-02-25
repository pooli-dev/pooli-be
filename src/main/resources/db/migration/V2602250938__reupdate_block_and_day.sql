DROP TABLE IF EXISTS `REPEAT_BLOCK_DAY`;
DROP TABLE IF EXISTS `REPEAT_BLOCK`;

CREATE TABLE `REPEAT_BLOCK` (
	`repeat_block_id`	BIGINT NOT NULL AUTO_INCREMENT,
	`line_id`	BIGINT	NOT NULL,
	`is_active`	BOOLEAN	NOT NULL	COMMENT '활성화 / 비활성화',
	`created_at`	TIMESTAMP	NOT NULL,
	`deleted_at`	TIMESTAMP	NULL,
	`updated_at`	TIMESTAMP	NULL,
	PRIMARY KEY (repeat_block_id),
    FOREIGN KEY (line_id) REFERENCES LINE (line_id)
);

CREATE TABLE `REPEAT_BLOCK_DAY` (
	`repeat_block_day_id`	BIGINT NOT NULL AUTO_INCREMENT,
	`repeat_block_id`	BIGINT	NOT NULL,
	`day_of_week`	ENUM('SUN','MON','TUE','WED','THU','FRI','SAT')	NOT NULL,
	`start_at`	TIME	NOT NULL,	
	`end_at`	TIME	NOT NULL,
	`created_at`	TIMESTAMP	NOT NULL,
	`deleted_at`	TIMESTAMP	NULL,
	`updated_at`	TIMESTAMP	NULL,
	PRIMARY KEY (repeat_block_day_id),
    FOREIGN KEY (repeat_block_id) REFERENCES REPEAT_BLOCK (repeat_block_id)
);



