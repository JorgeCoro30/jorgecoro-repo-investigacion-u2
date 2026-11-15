# Análisis experimental de ACK y entrega at-least-once en Pub/Sub

**Autor:** Jorge Luis Coro  
**Institución:** Escuela Superior Politécnica de Chimborazo (ESPOCH)  
**Facultad:** Informática y Electrónica  
**Carrera:** Telemática  
**Asignatura:** Sistemas Distribuidos y Paradigmas de la Computación  
**Nivel:** PAO 8

## Descripción

Este repositorio contiene el manuscrito académico, el conjunto de registros normalizados y el código de apoyo de un experimento sobre políticas de confirmación de mensajes en Google Cloud Pub/Sub. La experimentación reportada se realizó con el **emulador oficial local** (`127.0.0.1:8085`, proyecto local `local-pubsub-u2`); no se utilizaron recursos administrados de producción para obtener estas mediciones.

Se comparan tres escenarios:

- **A — ACK individual:** una solicitud `Acknowledge` por identificador.
- **B — Sin ACK:** no se envía `Acknowledge` ni `ModifyAckDeadline`; se observan reentregas.
- **C — ACK agrupado:** lotes de hasta ocho identificadores o envío por temporizador de 50 ms.

Tasas objetivo: 50 y 100 mensajes/s. Payload configurado: 4 KiB. La ventana formal prevista fue de 75 s, con publicación durante los primeros 45 s.

## Estado de la campaña y procedencia

El diseño contempló 60 corridas (10 bloques por tasa y tres escenarios). Durante la ejecución local, el proceso de campaña permaneció activo sin presentar un cierre/estado final inequívoco para todas las ejecuciones. La auditoría y los registros conservados permitieron recuperar 37 corridas con información verificable. La distribución del conjunto analizado es:

| Escenario | 50 msg/s | 100 msg/s |
|---|---:|---:|
| A | 6 | 6 |
| B | 6 | 7 |
| C | 6 | 6 |

Las ejecuciones sin evidencia completa no se incorporaron. No se imputaron ni generaron registros sintéticos. `results.csv` contiene 241 360 filas y conserva los eventos de entrega registrados. Los resultados deben interpretarse como observaciones del emulador local, no como mediciones de latencia o capacidad del servicio administrado.

## Estructura

```text
.
├── articulo.pdf                 # versión compilada para entrega
├── articulo.qmd                 # fuente del artículo
├── references.bib               # bibliografía BibTeX
├── _quarto.yml                  # configuración Quarto/IEEE
├── benchmark.py                 # punto de entrada
├── benchmark/                   # módulos originales del benchmark
├── results.csv                  # registros normalizados observados
├── README.md
├── PROMPTS_OPENCODE.md
├── ieee.csl
└── figura_*.png                 # figuras incluidas en el artículo
```

## Requisitos de reproducción

- Python 3.12 recomendado.
- Emulador oficial de Google Cloud Pub/Sub.
- Paquetes Python compatibles con los módulos de `benchmark/`: `google-cloud-pubsub`, `grpcio`, `pandas`, `numpy`, `scipy`, `matplotlib`.
- Quarto y una distribución LaTeX con `IEEEtran` para compilar el PDF.

Instalación orientativa:

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate
pip install google-cloud-pubsub grpcio pandas numpy scipy matplotlib
```

Iniciar el emulador en otra terminal:

```bash
gcloud beta emulators pubsub start --host-port=127.0.0.1:8085
```

Ejecutar una validación (crea una campaña nueva en la ruta indicada):

```bash
python benchmark.py campaign --out ./campaign-validation --validation
```

Ejecutar una campaña formal nueva (90 s nominales por corrida; 60 corridas planificadas):

```bash
python benchmark.py campaign --out ./campaign-formal
```

Recolectar los registros formales:

```bash
python benchmark.py collect ./campaign-formal ./campaign-output
```

**Advertencia:** estos comandos inician una nueva campaña. No son necesarios para consultar los resultados entregados y no deben ejecutarse sobre carpetas existentes salvo que se comprenda el modo de reanudación y sus controles. La campaña histórica no se debe volver a ejecutar para “completar” los datos publicados.

## Compilar el artículo

```bash
quarto render articulo.qmd --to pdf
```

El PDF entregado se corresponde con el manuscrito y las figuras del repositorio. Si se modifica `articulo.qmd`, volver a compilar para mantener sincronizados el PDF y la fuente.

## Archivos principales

- `articulo.qmd`: manuscrito en español con citas numéricas IEEE.
- `articulo.pdf`: versión de lectura.
- `results.csv`: observaciones normalizadas por entrega.
- `benchmark/`: implementación modular que generó y procesó los registros.
- `PROMPTS_OPENCODE.md`: trazabilidad resumida de la asistencia de IA y revisión.

## Limitaciones

1. Se analizaron 37 corridas verificables de 60 planificadas.
2. El emulador local no reproduce la infraestructura distribuida de Pub/Sub administrado.
3. La comparación inferencial por bloques no se declara concluyente por la cobertura parcial.
4. La latencia presentada en el artículo corresponde a la primera finalización del procesamiento, no al tiempo completo hasta confirmar.
5. La ausencia de ACK no es una modalidad oficial separada denominada “Unconfirmed Delivery”; es una política del consumidor que deja mensajes sin confirmar y permite reentregas.

## Alcance de los archivos publicados

El repositorio reúne el manuscrito, los datos observados y los componentes de software utilizados como apoyo para el experimento. Las cifras y conclusiones del artículo se limitan a las 37 corridas verificables realizadas en el emulador local. El código permite consultar el procedimiento y, si se desea realizar una nueva prueba, generar una campaña independiente; una nueva ejecución no modifica los resultados históricos incluidos en `results.csv`.
