from pydantic import BaseModel
from datetime import datetime

class RegistroEntrada(BaseModel):
    uuid: str
    marca_temporal: datetime
    descripcion: str
    importe: float
    tipo: str
