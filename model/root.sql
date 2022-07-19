
DROP TABLE IF EXISTS `user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user` (
  `uuid` varchar(8) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
  `name` varchar(64) NOT NULL DEFAULT '' COMMENT '姓名',
  `name_pinyin` varchar(512) NOT NULL DEFAULT '',
  `email` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT '' COMMENT '邮箱',
  `avatar` varchar(255) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT '' COMMENT '头像',
  `phone` varchar(32) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL COMMENT '手机号码',
  `password` varchar(60) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT '' COMMENT '密码',
  `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '1.正常 2.删除的 3.待激活 4.禁用的（被管理员禁用）',
  `create_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '创建时间',
  `modify_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '最后修改时间',
  PRIMARY KEY (`uuid`),
  UNIQUE KEY `unq_org_email` (`uuid`,`email`),
  KEY `idx_email` (`email`),
  KEY `idx_phone` (`phone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `group`;
CREATE TABLE `group` (
  `uuid` varchar(16) COLLATE latin1_bin NOT NULL,
  `owner` varchar(8) COLLATE latin1_bin DEFAULT NULL,
  `name` text CHARACTER SET utf8mb4 NOT NULL,
  `desc` longtext CHARACTER SET utf8mb4 COMMENT '分组描述',
  `create_time` bigint(20) NOT NULL DEFAULT '0',
  `status` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`uuid`),
  KEY `idx_owner` (`owner`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;


DROP TABLE IF EXISTS `task`;
CREATE TABLE `task` (
  `uuid` varchar(16) COLLATE latin1_bin NOT NULL,
  `owner` varchar(8) COLLATE latin1_bin DEFAULT NULL,
  `create_time` bigint(20) NOT NULL DEFAULT '0',
  `group_uuid` varchar(16) COLLATE latin1_bin DEFAULT NULL,
  `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '1.正常 2.删除的 3.待激活 4.禁用的（被管理员禁用）',
  `path` varchar(169) COLLATE latin1_bin NOT NULL COMMENT '支持10层子任务（n*17-1）',
  `name` text CHARACTER SET utf8mb4 NOT NULL,
  `desc` longtext CHARACTER SET utf8mb4 COMMENT '任务描述',
  `parent_uuid` varchar(16) COLLATE latin1_bin NOT NULL,
  PRIMARY KEY (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;