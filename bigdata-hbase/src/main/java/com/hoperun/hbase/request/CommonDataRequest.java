package com.hoperun.hbase.request;

public class CommonDataRequest<T>
{
    private T data;

    public T getData()
    {
	return data;
    }

    public void setData(T data)
    {
	this.data = data;
    }

    @Override
    public String toString()
    {
	return "CommonDataRequest [data=" + data + "]";
    }

}
