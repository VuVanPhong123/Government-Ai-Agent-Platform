'use client';
import { useMemo, useState } from 'react';
import Link from 'next/link';
import { useParams } from 'next/navigation';
import {
  AlertCircle,
  BarChart3,
  CircleDashed,
  Landmark,
  ShieldAlert,
  TrendingUp,
  Users,
} from 'lucide-react';
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import PageHeader from '@/components/ui/PageHeader';
import SectionCard from '@/components/ui/SectionCard';
import StateBlock from '@/components/ui/StateBlock';
import TableShell from '@/components/ui/TableShell';
import { ChartSkeleton, TableSkeleton } from '@/components/ui/Skeletons';
import { useCountries, useCountryAnalytics, useClusterBenchmark } from '@/lib/hooks/useCountries';
import { formatIndicatorValue, formatNullable, formatYear } from '@/lib/utils/format';

const tabs = [
  { id: 'tong-quan', label: 'Tổng quan' },
  { id: 'tang-truong', label: 'Tăng trưởng' },
  { id: 'tai-khoa', label: 'Tài khóa & tiền tệ' },
  { id: 'rui-ro', label: 'Rủi ro' },
  { id: 'xa-hoi', label: 'Xã hội' },
  { id: 'bang-du-lieu', label: 'Bảng dữ liệu' },
] as const;

const kpiConfig = [
  { key: 'actual_growth', label: 'Tăng trưởng', unit: '%', icon: TrendingUp },
  { key: 'actual_debt', label: 'Nợ công/GDP', unit: '%', icon: Landmark },
  { key: 'actual_inflation', label: 'Lạm phát CPI', unit: '%', icon: BarChart3 },
  { key: 'actual_unemployment', label: 'Thất nghiệp', unit: '%', icon: Users },
  { key: 'actual_poverty', label: 'Nghèo đa chiều', unit: '%', icon: ShieldAlert },
] as const;

function ChartBlock({
  title,
  data,
  lines,
}: {
  title: string;
  data: Array<Record<string, number | null>>;
  lines: Array<{ key: string; label: string; color: string }>;
}) {
  if (data.length === 0) {
    return (
      <div className="rounded-md border border-dashed border-slate-300 bg-slate-50 px-4 py-10 text-center text-sm text-slate-600">
        Không có dữ liệu để hiển thị biểu đồ.
      </div>
    );
  }

  return (
    <SectionCard title={title}>
      <div className="h-80 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis dataKey="year" />
            <YAxis />
            <Tooltip
              formatter={(value, key) => {
                const line = lines.find((item) => item.key === key);
                const numericValue =
                  typeof value === 'number' ? value : value == null ? null : Number(value);
                return [formatIndicatorValue(numericValue, '%'), line?.label || String(key)];
              }}
              labelFormatter={(label) => `Năm ${label}`}
            />
            <Legend />
            {lines.map((line) => (
              <Line
                key={line.key}
                type="monotone"
                dataKey={line.key}
                name={line.label}
                stroke={line.color}
                strokeWidth={2}
                dot={false}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </SectionCard>
  );
}

export default function CountryDetailPage() {
  const params = useParams();
  const code = String(params.code || '').toUpperCase();
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]['id']>('tong-quan');

  const countriesQuery = useCountries();
  const analyticsQuery = useCountryAnalytics(code);

  const rows = analyticsQuery.data?.data || [];
  const country = countriesQuery.data?.find((item) => item.country_code === code);
  const countryName = country?.country_name || code;

  const latestValidRow = useMemo(() => {
    const candidate = rows
      .slice()
      .reverse()
      .find(
        (item) =>
          item.actual_growth != null ||
          item.actual_debt != null ||
          item.actual_inflation != null ||
          item.actual_unemployment != null ||
          item.actual_poverty != null
      );
    return candidate || rows[rows.length - 1];
  }, [rows]);

  const benchmarkIndicator = 'govdebt_GDP';
  const benchmarkYear = latestValidRow?.year;
  const benchmarkQuery = useClusterBenchmark(code, benchmarkIndicator, benchmarkYear);

  const growthChartData = useMemo(
    () =>
      rows.map((item) => ({
        year: item.year,
        actual_growth: item.actual_growth ?? null,
        trend_growth: item.trend_growth ?? null,
      })),
    [rows]
  );

  const fiscalChartData = useMemo(
    () =>
      rows.map((item) => ({
        year: item.year,
        actual_debt: item.actual_debt ?? null,
        actual_inflation: item.actual_inflation ?? null,
      })),
    [rows]
  );

  const socialChartData = useMemo(
    () =>
      rows.map((item) => ({
        year: item.year,
        actual_unemployment: item.actual_unemployment ?? null,
        actual_poverty: item.actual_poverty ?? null,
      })),
    [rows]
  );

  const riskChartData = useMemo(
    () =>
      rows.map((item) => ({
        year: item.year,
        actual_reer_deviation: item.actual_reer_deviation ?? null,
        anomaly_reer_deviation: item.anomaly_reer_deviation ?? null,
      })),
    [rows]
  );

  const anomalies = useMemo(
    () =>
      rows
        .filter(
          (item) =>
            (item.anomaly_growth ?? 0) >= 0.75 ||
            (item.anomaly_debt ?? 0) >= 0.75 ||
            (item.anomaly_reer_deviation ?? 0) >= 0.75
        )
        .slice(-10)
        .reverse(),
    [rows]
  );

  if (analyticsQuery.isLoading || countriesQuery.isLoading) {
    return (
      <div className="space-y-4">
        <ChartSkeleton />
        <TableSkeleton rows={8} />
      </div>
    );
  }

  if (analyticsQuery.isError) {
    return (
      <StateBlock
        mode="error"
        title="Không tải được hồ sơ quốc gia"
        description={analyticsQuery.error instanceof Error ? analyticsQuery.error.message : 'Lỗi không xác định'}
      />
    );
  }

  if (!rows.length) {
    return (
      <StateBlock
        mode="empty"
        title="Chưa có dữ liệu hồ sơ quốc gia"
        description="API không trả dữ liệu phân tích đầy đủ cho quốc gia này."
      />
    );
  }

  return (
    <div className="space-y-5">
      <PageHeader
        title={`Hồ sơ quốc gia: ${countryName} (${code})`}
        description={`Năm dữ liệu gần nhất: ${formatYear(latestValidRow?.year)}`}
        actions={
          <div className="flex items-center gap-2">
            <Link
              href={`/compare?countries=${code},THA&indicator=govdebt_GDP&from=2010&to=2023`}
              className="rounded-md border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
            >
              So sánh với Thái Lan
            </Link>
            <Link
              href={`/chat?q=So%20s%C3%A1nh%20n%E1%BB%A3%20c%C3%B4ng%20${code}%20v%C3%A0%20THA%20t%E1%BB%AB%202010%20%C4%91%E1%BA%BFn%202023`}
              className="rounded-md border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
            >
              Hỏi trợ lý AI
            </Link>
          </div>
        }
      />

      <section className="grid grid-cols-1 gap-3 lg:grid-cols-6">
        {kpiConfig.map((item) => {
          const Icon = item.icon;
          const value = latestValidRow?.[item.key] as number | null | undefined;
          return (
            <div key={item.key} className="rounded-md border border-slate-200 bg-white px-4 py-3 shadow-sm">
              <div className="flex items-center gap-2 text-slate-600">
                <Icon className="h-4 w-4" />
                <p className="text-xs font-medium">{item.label}</p>
              </div>
              <p className="mt-2 text-lg font-semibold text-slate-900">{formatIndicatorValue(value, item.unit)}</p>
            </div>
          );
        })}
        <div className="rounded-md border border-slate-200 bg-white px-4 py-3 shadow-sm">
          <div className="flex items-center gap-2 text-slate-600">
            <CircleDashed className="h-4 w-4" />
            <p className="text-xs font-medium">Cụm cấu trúc</p>
          </div>
          <p className="mt-2 text-lg font-semibold text-slate-900">
            {latestValidRow?.cluster_id != null ? `Cụm ${latestValidRow.cluster_id}` : 'N/A'}
          </p>
        </div>
      </section>

      <div className="flex flex-wrap gap-2">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            type="button"
            onClick={() => setActiveTab(tab.id)}
            className={`rounded-md border px-3 py-1.5 text-sm ${
              activeTab === tab.id
                ? 'border-slate-400 bg-slate-100 text-slate-900'
                : 'border-slate-300 bg-white text-slate-700 hover:bg-slate-50'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === 'tong-quan' ? (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
          <SectionCard title="Thông tin quốc gia" className="xl:col-span-1">
            <dl className="space-y-3 text-sm">
              <div className="flex justify-between gap-3">
                <dt className="text-slate-600">Mã quốc gia</dt>
                <dd className="font-mono text-slate-900">{code}</dd>
              </div>
              <div className="flex justify-between gap-3">
                <dt className="text-slate-600">Tên quốc gia</dt>
                <dd className="text-slate-900">{countryName}</dd>
              </div>
              <div className="flex justify-between gap-3">
                <dt className="text-slate-600">Khu vực</dt>
                <dd className="text-slate-900">{formatNullable(country?.region)}</dd>
              </div>
              <div className="flex justify-between gap-3">
                <dt className="text-slate-600">Mức đầy đủ dữ liệu</dt>
                <dd className="text-slate-900">{formatNullable(analyticsQuery.data?.meta?.data_completeness)}</dd>
              </div>
            </dl>
          </SectionCard>

          <SectionCard title="So sánh trong cụm" className="xl:col-span-2">
            {benchmarkQuery.isLoading ? (
              <p className="text-sm text-slate-600">Đang tải benchmark cụm...</p>
            ) : benchmarkQuery.isError || !benchmarkQuery.data ? (
              <p className="text-sm text-slate-600">
                Chưa có dữ liệu benchmark cụm cho chỉ số nợ công/GDP ở năm {formatYear(benchmarkYear)}.
              </p>
            ) : (
              <div className="space-y-3">
                <p className="text-sm text-slate-700">
                  Trung bình cụm {benchmarkQuery.data.cluster_id}: {formatIndicatorValue(benchmarkQuery.data.average, '%')}
                </p>
                <TableShell>
                  <table className="min-w-full divide-y divide-slate-200 text-sm">
                    <thead className="bg-slate-50">
                      <tr>
                        <th className="px-3 py-2 text-left font-semibold">Quốc gia</th>
                        <th className="px-3 py-2 text-left font-semibold">Mã</th>
                        <th className="px-3 py-2 text-right font-semibold">Giá trị</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {benchmarkQuery.data.members.slice(0, 12).map((member) => (
                        <tr key={`${member.country_code}-${member.year}`}>
                          <td className="px-3 py-2">{member.country_name || member.country_code}</td>
                          <td className="px-3 py-2 font-mono">{member.country_code}</td>
                          <td className="px-3 py-2 text-right">{formatIndicatorValue(member.value, '%')}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </TableShell>
              </div>
            )}
          </SectionCard>

          <SectionCard title="Điểm bất thường gần đây" className="xl:col-span-3">
            {anomalies.length === 0 ? (
              <p className="text-sm text-slate-600">Không phát hiện bất thường vượt ngưỡng 0.75.</p>
            ) : (
              <ul className="space-y-2">
                {anomalies.map((item) => (
                  <li
                    key={`${item.year}-${item.country_code}-${item.anomaly_growth}-${item.anomaly_debt}-${item.anomaly_reer_deviation}`}
                    className="rounded border border-slate-200 bg-slate-50 px-3 py-2 text-sm"
                  >
                    <div className="flex items-center justify-between gap-3">
                      <span className="font-medium text-slate-800">Năm {item.year}</span>
                      <span className="text-xs text-slate-600">
                        Growth: {formatIndicatorValue(item.anomaly_growth ?? null, undefined, 3)} | Debt:{' '}
                        {formatIndicatorValue(item.anomaly_debt ?? null, undefined, 3)} | REER:{' '}
                        {formatIndicatorValue(item.anomaly_reer_deviation ?? null, undefined, 3)}
                      </span>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </SectionCard>
        </div>
      ) : null}

      {activeTab === 'tang-truong' ? (
        <ChartBlock
          title="Tăng trưởng GDP thực và xu hướng"
          data={growthChartData}
          lines={[
            { key: 'actual_growth', label: 'Tăng trưởng thực', color: '#1d4ed8' },
            { key: 'trend_growth', label: 'Xu hướng', color: '#0f766e' },
          ]}
        />
      ) : null}

      {activeTab === 'tai-khoa' ? (
        <ChartBlock
          title="Nợ công/GDP và lạm phát CPI"
          data={fiscalChartData}
          lines={[
            { key: 'actual_debt', label: 'Nợ công/GDP', color: '#b45309' },
            { key: 'actual_inflation', label: 'Lạm phát CPI', color: '#7c3aed' },
          ]}
        />
      ) : null}

      {activeTab === 'rui-ro' ? (
        <ChartBlock
          title="Độ lệch REER và điểm bất thường"
          data={riskChartData}
          lines={[
            { key: 'actual_reer_deviation', label: 'Độ lệch REER', color: '#dc2626' },
            { key: 'anomaly_reer_deviation', label: 'Điểm bất thường REER', color: '#0f766e' },
          ]}
        />
      ) : null}

      {activeTab === 'xa-hoi' ? (
        <ChartBlock
          title="Thất nghiệp và nghèo đa chiều"
          data={socialChartData}
          lines={[
            { key: 'actual_unemployment', label: 'Thất nghiệp', color: '#0f766e' },
            { key: 'actual_poverty', label: 'Nghèo đa chiều', color: '#b45309' },
          ]}
        />
      ) : null}

      {activeTab === 'bang-du-lieu' ? (
        <TableShell>
          <table className="min-w-full divide-y divide-slate-200 text-sm">
            <thead className="bg-slate-50">
              <tr>
                <th className="px-3 py-2 text-left font-semibold">Năm</th>
                <th className="px-3 py-2 text-right font-semibold">Tăng trưởng</th>
                <th className="px-3 py-2 text-right font-semibold">Nợ công/GDP</th>
                <th className="px-3 py-2 text-right font-semibold">Lạm phát</th>
                <th className="px-3 py-2 text-right font-semibold">Thất nghiệp</th>
                <th className="px-3 py-2 text-right font-semibold">Nghèo đa chiều</th>
                <th className="px-3 py-2 text-right font-semibold">Cụm</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 bg-white">
              {rows.map((item) => (
                <tr key={`${item.country_code}-${item.year}`}>
                  <td className="px-3 py-2 font-mono">{formatYear(item.year)}</td>
                  <td className="px-3 py-2 text-right">{formatIndicatorValue(item.actual_growth ?? null, '%')}</td>
                  <td className="px-3 py-2 text-right">{formatIndicatorValue(item.actual_debt ?? null, '%')}</td>
                  <td className="px-3 py-2 text-right">{formatIndicatorValue(item.actual_inflation ?? null, '%')}</td>
                  <td className="px-3 py-2 text-right">
                    {formatIndicatorValue(item.actual_unemployment ?? null, '%')}
                  </td>
                  <td className="px-3 py-2 text-right">{formatIndicatorValue(item.actual_poverty ?? null, '%')}</td>
                  <td className="px-3 py-2 text-right">{item.cluster_id ?? 'N/A'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableShell>
      ) : null}

      {benchmarkQuery.isError ? (
        <div className="rounded-md border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          <div className="flex items-start gap-2">
            <AlertCircle className="mt-0.5 h-4 w-4" />
            <span>Không tải được dữ liệu benchmark cụm. Trang hồ sơ vẫn hoạt động với dữ liệu chính.</span>
          </div>
        </div>
      ) : null}
    </div>
  );
}
