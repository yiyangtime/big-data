package com.hoperun.hbase.request;

public class CommonPageRequestBody
{
    private Integer offset;
    private Integer limit;

    public Integer getOffset()
    {
	return offset;
    }

    public void setOffset(Integer offset)
    {
	this.offset = offset;
    }

    public Integer getLimit()
    {
	return limit;
    }

    public void setLimit(Integer limit)
    {
	this.limit = limit;
    }

    @Override
    public String toString()
    {
	StringBuilder builder = new StringBuilder();
	builder.append("CommonPageRequestBody [offset=");
	builder.append(offset);
	builder.append(", limit=");
	builder.append(limit);
	builder.append("]");
	return builder.toString();
    }

}
