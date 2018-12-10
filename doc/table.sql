create table api_service.forder_aggregation(
        `order_no` varchar(50),
        `loan_type` varchar(32),
        `product_purpose` varchar(64),
        `product_name` varchar(100),
        `order_status` int(4),
        `seller_name` varchar(64),
        `order_commit_time` datetime,
        `sign_time` datetime,
        `end_time` datetime,
        `participant_id` varchar(50),
        `use_amount` decimal(16,2),
        `fund_use` varchar(128),
        `borrow_use` int(4),
        `back_way` varchar(16),
        `pay_time` datetime,
        `bank_account_name` varchar(64),
        `bank_account_number` varchar(32),
        `opening_bank` varchar(256),
        `branch_bank` varchar(256),
        `pledge_agency_type` tinyint(1),
        `bank_name` varchar(128),
        `branch_name` varchar(128),
        `is_order_repayment` tinyint(1),
        `order_repayment_date` datetime,
        `account_manager` varchar(64),
        `account_manager_tel` varchar(32),
        `property_pledge_info` tinyint(1),
        `debts_district` varchar(64),
        `house_district` varchar(64),
        `pledge_type` varchar(128),
        `order_main_update_time` datetime DEFAULT NULL,
        `order_sellerinfo_update_time` datetime DEFAULT NULL,
        `order_appendinfo_update_time` datetime DEFAULT NULL,
        `participant_update_time` datetime DEFAULT NULL,
		`order_contract_update_time` datetime DEFAULT NULL,
		`order_reddemhouse_update_time` datetime DEFAULT NULL,
        `update_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY ( `order_no` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
ALTER TABLE `forder_aggregation` ADD INDEX main_update_time (`order_main_update_time`);
ALTER TABLE `forder_aggregation` ADD INDEX sellerinfo_update_time (`order_sellerinfo_update_time`);
ALTER TABLE `forder_aggregation` ADD INDEX appendinfo_update_time (`order_appendinfo_update_time`);
ALTER TABLE `forder_aggregation` ADD INDEX participant_update_time (`participant_update_time`);
ALTER TABLE `forder_aggregation` ADD INDEX contract_update_time (`order_contract_update_time`);
ALTER TABLE `forder_aggregation` ADD INDEX reddemhouse_update_time (`order_reddemhouse_update_time`);


CREATE TABLE IF NOT EXISTS api_service.lft_report(
   `id` INT UNSIGNED AUTO_INCREMENT,
   `all_amt` double(16,2),
   `all_amt_today` double(16,2),
   `other_amt` double(16,2),
   `sub_acc_amt` double(16,2),
   `agent_amt` double(16,2),
   `agreement_amt` double(16,2),
   `data_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   PRIMARY KEY ( `id` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
insert into api_service.lft_report(all_amt,all_amt_today,other_amt,sub_acc_amt,agent_amt,agreement_amt) values(0,0,0,0,0,0);
