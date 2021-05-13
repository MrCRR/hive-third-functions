package com.chinagoods.bigdata.functions.session;

import com.chinagoods.bigdata.functions.regexp.Re2JRegexp;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDFSessionRowSequence.
 * @author xiaowei.song
 */
@Description(name = "session_row_sequence",
        value = "_FUNC_(distinct_id, event, created_at) - Returns a generated row sequence number starting from 1")
@UDFType(deterministic = false, stateful = true)
public class UDFSessionRowSequence extends UDF {
    private static final Logger log = LoggerFactory.getLogger(Re2JRegexp.class);

    // 间隔超过60s，认为会话已结束，重新开始
    public static final Long MAX_SESSION_INTERVAL_SEC = 60 * 1000L;

    public static final String APP_START_NAME = "$AppStart";
    public static final String APP_END_NAME = "$AppEnd";

    private static volatile Long result = 0L;
    private static volatile Long createdAt = 0L;
    private static volatile String distinctId = "";
    private static volatile Boolean appEndFlag = Boolean.FALSE;

    public UDFSessionRowSequence() {
        appEndFlag = Boolean.FALSE;
    }

    public Long evaluate(String distinctIdStr, String eventNameStr, String iCreatedAt) {
        log.info(String.format("distinctId=%s,  event=%s,   createdAt=%s", distinctIdStr, eventNameStr, iCreatedAt));
        Long createdAtLong = Long.valueOf(iCreatedAt);

        if (StringUtils.isBlank(distinctIdStr)) {
            distinctIdStr = "";
        }

        if (StringUtils.isBlank(eventNameStr)) {
            eventNameStr = "";
        }

        // 起始阶段赋值
        if (result == 0) {
            result++;
            setLagStatus(distinctIdStr, createdAtLong);
        }

        String lagDistinctId = distinctId;
        Long lagCreatedAt = createdAt;


        // 若上一个distinct_id、platformType、platformLang跟此时不一致，则重置row_number
        if (!lagDistinctId.equals(distinctIdStr)) {
            result = 0L;
            result++;
        } else if (APP_START_NAME.equals(eventNameStr) ||
                createdAtLong - lagCreatedAt > MAX_SESSION_INTERVAL_SEC ||
                appEndFlag) {
            appEndFlag = false;
            result++;
        }

        if (APP_END_NAME.equals(eventNameStr)) {
            appEndFlag = true;
        }
        setLagStatus(distinctIdStr, createdAtLong);
        return result;
    }

    private void setLagStatus(String distinctIdStr, Long createdAtLong) {
        createdAt = createdAtLong;
        distinctId = distinctIdStr;
    }
}
