"use client";

import type { CustomerOption } from "@/lib/customer-events-store";

interface CustomerPickerProps {
  customers: CustomerOption[];
  slug: string;
}

export function CustomerPicker({ customers, slug }: CustomerPickerProps) {
  return (
    <select
      aria-label="Customer"
      className="h-9 min-w-48 rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50"
      value={slug}
      onChange={(event) => {
        window.location.assign(`/customers/${event.currentTarget.value}`);
      }}
    >
      {customers.map((customer) => (
        <option key={customer.slug} value={customer.slug}>
          {customer.name}
        </option>
      ))}
    </select>
  );
}
