package io.github.kamenriderkuuga.ojson;

import java.util.Map;

/**
 * 数据解析结果
 *
 * @author guohao
 * @date 2023/2/8
 */
public class ParseResult {
    /**
     * 解析结果
     */
    private boolean success;

    /**
     * 解析错误时的错误信息
     */
    private String message;

    /**
     * 解析后的数据
     */
    private Map<String, Object> data;

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public boolean getSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, Object> getData() {
        return data;
    }
}
