'use client';
import { useParams, useRouter } from 'next/navigation';
import { useCountryAnalytics, useCountries } from '@/lib/hooks/useCountries';
import { ChartSkeleton, KpiCardSkeleton, PanelSkeleton, TableSkeleton } from '@/components/ui/Skeletons';
import CountryHeader from '@/components/country/CountryHeader';
import CountryKpiOverview from '@/components/country/CountryKpiOverview';
import Tabs from '@/components/ui/Tabs';
import CountryInfoPanel from '@/components/country/CountryInfoPanel';
import ClusterRankingPanel from '@/components/country/ClusterRankingPanel';
import RecentAnomaliesPanel from '@/components/country/RecentAnomaliesPanel';
import ClusterBenchmarkPanel from '@/components/country/ClusterBenchmarkPanel';
import GrowthTabContent from '@/components/country/GrowthTabContent';
import FiscalTabContent from '@/components/country/FiscalTabContent';
import SocialTabContent from '@/components/country/SocialTabContent';
import RiskTabContent from '@/components/country/RiskTabContent';
import CountryDataTable from '@/components/country/CountryDataTable';
import { useMemo, useState } from 'react';

interface TabItem { id: string; label: string; status?: 'ok' | 'warning' | 'error'; content: React.ReactNode; }

const INDICATOR_MAP: Record<string, string> = { growth: 'rGDP_growth_YoY', fiscal: 'govdebt_GDP', social: 'unemployment_total', risk: 'actual_reer_deviation' };

export default function CountryDetailPage() {
  const router = useRouter();
  const { code } = useParams();
  const normalizedCode = (code as string).toUpperCase();
  const [activeTab, setActiveTab] = useState('growth');
  const { data: analyticsPayload, isLoading, isError, error } = useCountryAnalytics(normalizedCode);
  const { data: countries } = useCountries();

  const data = analyticsPayload?.data || [];

  const countryInfo = useMemo(() => countries?.find(c => c.country_code === normalizedCode), [countries, normalizedCode]);
  const latestYear = data && data.length > 0 ? data[data.length - 1].year : null;
  const countryName = countryInfo?.country_name || normalizedCode;
  const latestRow = data && data.length > 0 ? data[data.length - 1] : null;

  const anomalies = useMemo(() => {
    if (!data) return [];
    const res: any[] = [];
    data.forEach((d: any) => {
      if ((d.anomaly_growth ?? 0) >= 0.75) res.push({ year: d.year, indicator: 'rGDP_growth_YoY', score: d.anomaly_growth });
      if ((d.anomaly_debt ?? 0) >= 0.75) res.push({ year: d.year, indicator: 'govdebt_GDP', score: d.anomaly_debt });
      if ((d.anomaly_reer_deviation ?? 0) >= 0.75) res.push({ year: d.year, indicator: 'actual_reer_deviation', score: d.anomaly_reer_deviation });
    });
    return res;
  }, [data]);

  if (isLoading) {
    return (
      <div className="space-y-6">
        <div className="h-16 bg-white rounded-md animate-pulse" />
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => <KpiCardSkeleton key={i} />)}
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          <div className="lg:col-span-8"><ChartSkeleton /></div>
          <div className="lg:col-span-4 space-y-4"><PanelSkeleton rows={3} /><PanelSkeleton rows={4} /></div>
        </div>
        <TableSkeleton rows={5} />
      </div>
    );
  }
  if (isError) return <div className="p-6 bg-red-50 text-red-700 rounded border border-red-200">Error: {error?.message}</div>;
  if (!data || data.length === 0) return <div className="p-12 text-center bg-white rounded-md border border-gray-200">Không có dữ liệu cho quốc gia này.</div>;

  const tabs: TabItem[] = [
    { id: 'growth', label: 'Tăng trưởng', status: data.some((d: any) => d.actual_growth != null) ? 'ok' : undefined, content: <GrowthTabContent data={data} /> },
    { id: 'fiscal', label: 'Tài khóa & Tiền tệ', status: data.some((d: any) => d.actual_debt != null) ? 'ok' : 'warning', content: <FiscalTabContent data={data} /> },
    { id: 'social', label: 'Xã hội', status: data.some((d: any) => d.actual_unemployment != null) ? 'ok' : 'warning', content: <SocialTabContent data={data} /> },
    { id: 'risk', label: 'Rủi ro', status: data.some((d: any) => d.actual_reer_deviation != null) ? 'ok' : 'error', content: <RiskTabContent data={data} /> },
  ];

  return (
    <div className="space-y-6">
      <CountryHeader countryCode={normalizedCode} countryName={countryName} latestYear={latestYear} onExport={() => console.log('Export')} onCompare={() => router.push(`/compare?countries=${normalizedCode}&indicator=rGDP_growth_YoY`)} />
      <CountryKpiOverview data={data} />
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        <div className="lg:col-span-8">
          <Tabs tabs={tabs} defaultActiveId={activeTab} onChange={setActiveTab} />
        </div>
        <div className="lg:col-span-4 space-y-4">
          <CountryInfoPanel
            code={normalizedCode}
            region={countryInfo?.region}
            dataCompleteness={analyticsPayload?.meta?.data_completeness}
          />
          <ClusterRankingPanel clusterId={latestRow?.cluster_id} />
          <ClusterBenchmarkPanel countryCode={normalizedCode} clusterId={latestRow?.cluster_id} year={latestYear ?? undefined} activeIndicator={INDICATOR_MAP[activeTab] || 'rGDP_growth_YoY'} />          <RecentAnomaliesPanel anomalies={anomalies} />
        </div>
      </div>
      <CountryDataTable data={data} />
    </div>
  );
}