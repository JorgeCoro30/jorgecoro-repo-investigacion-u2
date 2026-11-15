import sys, os, json
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

runs = json.load(open(os.path.join(sys.argv[1], 'runs.json')))
df = pd.DataFrame(runs)
valid = df[df['operational_valid'] == True].copy()

out = os.path.join(sys.argv[1], 'figuras')
os.makedirs(out, exist_ok=True)

METRICS = [
    ('ratio_ack_rpc_per_umsg', 'RPC ACK / mensaje \u00fanico recibido', 'ack_rpc'),
    ('ratio_ack_rpc_per_ok', 'RPC ACK / mensaje \u00fanico completado', 'ack_completado'),
    ('goodput_per_s', 'Goodput (mensajes \u00fanicos/s)', 'goodput'),
    ('latency_p95_ms', 'Latencia p95 de primera finalizaci\u00f3n (ms)', 'latencia'),
    ('amplification', 'Entregas / mensaje \u00fanico recibido', 'amplificacion'),
]
GROUPS = [
    (1, 'A individual'),
    (2, 'B sin ACK'),
    (3, 'C agrupado'),
]
RATES = [50, 100]
SUB = 'Emulador Pub/Sub \u2014 corridas formales; una observaci\u00f3n por corrida'

fmt = lambda v: f'{v:.2f}' if abs(v) >= 10 else f'{v:.3f}'

def last_value(val):
    if isinstance(val, (list, tuple, np.ndarray)):
        arr = [x for x in val if x is not None and not (isinstance(x, float) and np.isnan(x))]
        return arr[-1] if arr else np.nan
    return val

manifest_metrics = {m[0]: {'label': m[1], 'file': m[2]} for m in METRICS}

for metric, ylabel, fkey in METRICS:
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
    dvals = {}
    for rate in RATES:
        sub = valid[valid['rate_target'] == rate]
        for g in (1, 2, 3):
            letter = 'ABC'[g - 1]
            gsub = sub[sub['scenario'] == letter]
            raw = gsub[metric] if metric in gsub.columns else pd.Series(dtype=float)
            vals = [last_value(v) for v in raw]
            vals = [float(v) for v in vals if v is not None and not (isinstance(v, float) and np.isnan(v))]
            dvals[(rate, g)] = np.array(vals, dtype=float)

    for ax, rate in zip(axes, RATES):
        data = [dvals[(rate, g)] for g in (1, 2, 3)]
        pos = [g for g in (1, 2, 3) if len(dvals[(rate, g)]) > 0]
        nonempty = [d for d in data if len(d) > 0]
        if nonempty:
            bp = ax.boxplot(nonempty, positions=pos, widths=0.5,
                            patch_artist=True, showfliers=False,
                            medianprops=dict(color='black', lw=1.4),
                            whiskerprops=dict(color='#444'), capprops=dict(color='#444'))
            for patch in bp['boxes']:
                patch.set_facecolor('#dbe7f3'); patch.set_edgecolor('#444')
        rng = np.random.default_rng(20261114)
        for g in (1, 2, 3):
            v = dvals[(rate, g)]
            if len(v) > 0:
                jit = rng.uniform(-0.07, 0.07, size=len(v))
                ax.scatter(g + jit, v, s=22, color='#d1495b', alpha=0.85,
                           edgecolors='white', linewidths=0.4, zorder=3)
            if len(v) == 0:
                if metric.startswith('ratio_ack') and g == 2:
                    lbl = 'No aplicable'
                else:
                    lbl = 'Sin datos'
                ax.text(g, 0.1, lbl, transform=ax.get_xaxis_transform(),
                        ha='center', va='center', fontsize=9, style='italic', color='#555')
        counts = [len(dvals[(rate, g)]) for g in (1, 2, 3)]
        ax.set_xticks([1, 2, 3])
        ax.set_xticklabels([f'{GROUPS[i][1]}\nn={counts[i]}' for i in range(3)], fontsize=9)
        ax.set_title(f'{rate} mensajes/s', fontsize=11)
        ax.grid(True, axis='y', linestyle=':', alpha=0.6)
        ax.set_xlim(0.4, 3.6)
    axes[0].set_ylabel(ylabel)
    fig.suptitle(SUB, fontsize=11)
    fig.tight_layout()
    png = os.path.join(out, f'fig_{fkey}.png')
    svg = os.path.join(out, f'fig_{fkey}.svg')
    fig.savefig(png, dpi=180, bbox_inches='tight')
    fig.savefig(svg, bbox_inches='tight')
    plt.close(fig)

    manifest_metrics[metric]['paths'] = {'png': png, 'svg': svg}
    manifest_metrics[metric]['n_valid'] = {
        str(rate): {'ABC'[g - 1]: int(len(dvals[(rate, g)])) for g in (1, 2, 3)}
        for rate in RATES
    }

manifest = {
    'description': SUB,
    'corridas': int(len(valid)),
    'censura': 'Latencia condicional a completados; no se imputan p\u00e9rdidas como 0.',
    'metrics': manifest_metrics,
}
with open(os.path.join(sys.argv[1], 'figures_manifest.json'), 'w', encoding='utf-8') as fh:
    json.dump(manifest, fh, ensure_ascii=False, indent=2)
