from pydantic import BaseModel, Field


class Propertie(BaseModel):
    code: str = Field(min_length=1)
    dpId: int  
    time: int  = Field(gt=0)
    value: int|float 


class BizData(BaseModel) :
    devId : str = Field(min_length=0)
    dataId :  str 
    productId: str = Field(min_length=1)
    properties: list[Propertie]


class DevMessage(BaseModel) :
    bizCode: str
    bizData: BizData
    ts: int = Field(gt=0)


class CommandOnOFF(BaseModel):
    device_id: str = Field(min_length=1)
    switch: bool







