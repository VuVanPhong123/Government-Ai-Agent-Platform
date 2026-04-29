'use client';
import { useUIStore } from "@/lib/stores/uiStore";

export default function Header() {
  const { toggleSidebar } = useUIStore();
  return (
    <header className="bg-white shadow-sm py-4 px-4 lg:px-6 flex items-center justify-between">
      <button onClick={toggleSidebar} className="lg:hidden p-2 border rounded hover:bg-gray-100">
        ☰
      </button>
      <h2 className="text-xl font-semibold text-gray-800">Dashboard</h2>
    </header>
  );
}