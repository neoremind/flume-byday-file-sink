package com.baidu.unbiz.flume.sink;

/**
 * @author zhangxu
 */
public class InputNotSpecifiedException extends RuntimeException {
    private static final long serialVersionUID = 1102327497549834945L;

    public InputNotSpecifiedException() {
    }

    public InputNotSpecifiedException(String message) {
        super(message);
    }

    public InputNotSpecifiedException(String message, Throwable t) {
        super(message, t);
    }

    public InputNotSpecifiedException(Throwable t) {
        super(t);
    }
}
