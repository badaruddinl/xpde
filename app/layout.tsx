import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "XPDE — GOLDm# Shadow Terminal",
  description:
    "Probabilistic decision support for GOLDm# with calibrated uncertainty and manual confirmation.",
  icons: {
    icon: "/favicon.svg",
    shortcut: "/favicon.svg",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="id">
      <body>{children}</body>
    </html>
  );
}
