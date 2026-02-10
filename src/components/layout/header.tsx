"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { Settings } from "lucide-react";
import { cn } from "@/lib/utils";

interface NavItem {
  href: string;
  label: string;
  isActive: (pathname: string, searchParams: URLSearchParams) => boolean;
}

const navItems: NavItem[] = [
  {
    href: "/",
    label: "Gallery",
    isActive: (pathname, searchParams) =>
      pathname === "/" && !searchParams.has("agent"),
  },
  {
    href: "/highlights",
    label: "Highlights",
    isActive: (pathname) => pathname.startsWith("/highlights"),
  },
  {
    href: "/?agent=true",
    label: "Agent",
    isActive: (pathname, searchParams) =>
      pathname === "/" && searchParams.has("agent"),
  },
];

interface HeaderProps {
  children?: React.ReactNode;
}

export function Header({ children }: HeaderProps) {
  const pathname = usePathname();
  const searchParams = useSearchParams();

  return (
    <header className="sticky top-0 z-50 w-full shadow-sm bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container mx-auto px-4 h-12 flex items-center justify-between">
        <div className="flex items-center gap-6">
          <Link href="/" className="flex items-center">
            <Image
              src="/logo-dark.svg"
              alt="Bee AI Events"
              width={28}
              height={28}
              className="dark:hidden"
            />
            <Image
              src="/logo-light.svg"
              alt="Bee AI Events"
              width={28}
              height={28}
              className="hidden dark:block"
            />
          </Link>
          <nav className="flex items-center gap-1">
            {navItems.map((item) => {
              const active = item.isActive(pathname, searchParams);
              return (
                <Link
                  key={item.label}
                  href={item.href}
                  className={cn(
                    "px-3 py-1.5 rounded-md text-sm font-medium transition-colors",
                    active
                      ? "bg-primary/10 text-primary"
                      : "text-muted-foreground hover:text-foreground hover:bg-muted"
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </div>
        <div className="flex items-center gap-2">
          {children}
          <Link
            href="/settings"
            className={cn(
              "inline-flex items-center justify-center rounded-md h-9 w-9 transition-colors",
              pathname.startsWith("/settings")
                ? "bg-primary/10 text-primary"
                : "text-muted-foreground hover:text-foreground hover:bg-muted"
            )}
          >
            <Settings className="w-4 h-4" />
          </Link>
        </div>
      </div>
    </header>
  );
}
