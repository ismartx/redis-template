package org.smartx.redis;

/**
 * redis exception
 *
 * @author kext
 */
public class RedisException extends Exception {

    private static final long serialVersionUID = 7115322404472565037L;

    public RedisException(){
        super();
    }

    public RedisException(String message, Throwable cause){
        super(message, cause);
    }

    public RedisException(String message){
        super(message);
    }

    public RedisException(Throwable cause){
        super(cause);
    }

}
