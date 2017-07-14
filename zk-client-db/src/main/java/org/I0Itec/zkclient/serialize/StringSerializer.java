package org.I0Itec.zkclient.serialize;

import org.I0Itec.zkclient.exception.ZkMarshallingError;

import java.io.UnsupportedEncodingException;

/**
 * Created by natalia on 7/13/17.
 */
public class StringSerializer implements ZkSerializer {
	@Override
	public byte[] serialize(Object data) throws ZkMarshallingError {
		try {
			return data.toString().getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new ZkMarshallingError( "Can't serialize object " + data.getClass().getName() );
		}
	}

	@Override
	public Object deserialize(byte[] bytes) throws ZkMarshallingError {
		try {
			return new String(bytes,"UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new ZkMarshallingError();
		}
	}
}
