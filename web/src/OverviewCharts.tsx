import { locale, t } from './locale.ts';
import { useEffect, useMemo, useRef, useState } from 'react';
import { ChartNoAxesCombined, Table2 } from 'lucide-react';
import { maxSampleGapMs, trendsInWindow, type ChartPoint, type RuntimeSample } from './overviewSeries';
import { Modal } from './ui';

const colors = ['#2855d9', '#a04e17'];
const format = new Intl.NumberFormat(locale, { maximumFractionDigits: 1 });
const clock = new Intl.DateTimeFormat(locale, { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });

function TrendPlot({ points }: { points: ChartPoint[] }) {
  const ref = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    const canvas = ref.current;
    const ctx = canvas?.getContext('2d');
    if (!canvas || !ctx) return;
    const first = points[0].at;
    const last = points[points.length - 1].at;
    const max = Math.max(2, Math.ceil(Math.max(...points.flatMap(({ values }) => values ?? [0])) / 2) * 2);
    function draw(width: number, height: number) {
      if (width <= 0 || height <= 0 || !ctx || !canvas) return;
      const ratio = window.devicePixelRatio || 1;
      canvas.width = Math.round(width * ratio);
      canvas.height = Math.round(height * ratio);
      ctx.scale(ratio, ratio);
      ctx.fillStyle = '#5d687a';
      ctx.font = '11px system-ui, sans-serif';
      const left = ctx.measureText(format.format(max)).width + 10;
      const right = width - 7;
      const top = 10;
      const bottom = height - 9;
      const y = (value: number) => bottom - value / max * (bottom - top);
      const x = (at: number) => first === last ? (left + right) / 2 : left + (at - first) / (last - first) * (right - left);
      ctx.textBaseline = 'middle';
      for (const tick of [0, max / 2, max]) {
        ctx.fillText(format.format(tick), 0, y(tick));
        ctx.strokeStyle = '#e2e6ed';
        ctx.beginPath();
        ctx.moveTo(left, y(tick));
        ctx.lineTo(right, y(tick));
        ctx.stroke();
      }
      colors.forEach((color, series) => {
        ctx.strokeStyle = ctx.fillStyle = color;
        ctx.lineWidth = 2;
        ctx.setLineDash(series === 0 ? [] : [5, 4]);
        ctx.beginPath();
        points.forEach(({ at, values }, i) => {
          if (!values) return;
          const previous = points[i - 1];
          if (previous?.values && at - previous.at <= maxSampleGapMs) ctx.lineTo(x(at), y(values[series]));
          else ctx.moveTo(x(at), y(values[series]));
        });
        ctx.stroke();
        for (const { at, values } of points) {
          if (!values) continue;
          ctx.beginPath();
          ctx.arc(x(at), y(values[series]), series === 0 ? 3 : 2, 0, Math.PI * 2);
          ctx.fill();
        }
      });
    }
    draw(canvas.clientWidth, canvas.clientHeight);
    const observer = new ResizeObserver(([entry]) => draw(entry.contentRect.width, entry.contentRect.height));
    observer.observe(canvas);
    return () => observer.disconnect();
  }, [points]);
  return <canvas className="trend-plot" ref={ref} aria-hidden="true" />;
}

function Trend({ title, points, labels, empty, active }: {
  title: string; points: ChartPoint[]; labels: [string, string]; empty: string; active: boolean;
}) {
  const [inspecting, setInspecting] = useState<ChartPoint[]>();
  useEffect(() => {
    if (!active) setInspecting(undefined);
  }, [active]);
  const latest = points.at(-1)?.values;
  const showPlot = points.some(point => point.values);
  return <figure className="overview-panel trend">
    <figcaption className="overview-section-heading">
      <h3>{title}</h3>
      <button type="button" className="text-button" disabled={!points.length} aria-label={t('查看{0}采样数据', title)}
        aria-haspopup="dialog" onClick={() => setInspecting(points)}>
        <Table2 size={15} aria-hidden="true" />{t('查看数据')}
      </button>
    </figcaption>
    <ul className="trend-legend">
      {labels.map((label, index) => <li key={label}><i aria-hidden="true" />{label} <strong>{latest ? format.format(latest[index]) : '—'}</strong></li>)}
    </ul>
    {showPlot ? <>
      <TrendPlot points={points} />
      <div className="trend-times"><span>{clock.format(points[0].at)}</span>
        {points.length > 1 && <span>{clock.format(points[points.length - 1].at)}</span>}
      </div>
    </> : <p className="trend-empty muted">{empty}</p>}
    {active && inspecting && <Modal
      title={t('{0}采样数据', title)}
      description={t('{0} 个采样点 · 按时间从近到远 · “—”表示该时刻无有效值', inspecting.length)}
      close={() => setInspecting(undefined)}>
      <div className="panel-body trend-data" tabIndex={0} role="region" aria-label={t('{0}采样表', title)}>
        <table>
          <caption className="sr-only">{title}{t('采样数据')}</caption>
          <thead><tr><th scope="col">{t('时间')}</th>{labels.map(label => <th scope="col" key={label}>{label}</th>)}</tr></thead>
          <tbody>{[...inspecting].reverse().map(({ at, values }) => <tr key={at}>
            <th scope="row"><time dateTime={new Date(at).toISOString()}>{clock.format(at)}</time></th>
            {labels.map((label, index) => <td key={label}>{values ? format.format(values[index]) : '—'}</td>)}
          </tr>)}</tbody>
        </table>
      </div>
    </Modal>}
  </figure>;
}

export function OverviewCharts({ samples, active }: { samples: RuntimeSample[]; active: boolean }) {
  const [minutes, setMinutes] = useState(15);
  const trends = useMemo(() => trendsInWindow(samples, minutes), [samples, minutes]);
  return <section className="overview-trends">
    <div className="overview-section-heading">
      <h2><ChartNoAxesCombined size={17} aria-hidden="true" />{t('运行趋势')}</h2>
      <label className="trend-period">
        <span className="sr-only">{t('趋势时间范围')}</span>
        <select value={minutes} onChange={event => setMinutes(Number(event.target.value))}>
          <option value={5}>{t('最近 5 分钟')}</option>
          <option value={15}>{t('最近 15 分钟')}</option>
        </select>
      </label>
    </div>
    <div className="trend-grid">
      <Trend title={t('查询负载')} points={trends.query} labels={[t('运行中'), t('等待中')]} empty={t('暂无查询负载样本')} active={active} />
      <Trend title={t('HTTP 请求')} points={trends.requests} labels={[t('请求 / 分钟'), t('5xx / 分钟')]} empty={t('等待两次连续采集后计算请求速率')} active={active} />
    </div>
  </section>;
}
