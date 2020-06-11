package com.shrek.test.flink.entity;

import lombok.Data;

/**
 * TODO
 *
 * @author WuShu
 * @date 2020-05-21 17:24
 * @remark
 */
@Data
public class UserPV {

    private String app;
    private String businesscode;
    private String ip;
    private long logTime;
    private String memberId;
    private String openid;
    private String storedId;

    private int cnt = 1;

}
