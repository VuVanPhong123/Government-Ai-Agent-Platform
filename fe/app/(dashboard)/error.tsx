'use client';
import { useEffect } from 'react';

export default function DashboardError({ error, reset }: { error: Error & { digest?: string }, reset: () => void }) {
  useEffect(() => {
    console.error('Dashboard Error:', error);
  }, [error]);

  return (
    <div className="p-6 bg-red-50 border border-red-200 rounded-lg">
      <h2 className="text-lg font-semibold text-red-800">Đã xảy ra lỗi tải dữ liệu</h2>
      <p className="text-red-600 mt-2 text-sm">{error.message}</p>
      <button onClick={reset} className="mt-4 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700 text-sm">
        Thử lại
      </button>
    </div>
  );
}