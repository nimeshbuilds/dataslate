import { AlertCircle, ArrowRight, Check, LoaderCircle, X } from 'lucide-react';
import { useEffect, useRef } from 'react';
import type { ReactNode } from 'react';

export function Logo({ small = false }: { small?: boolean }) {
  return <span className={`brand ${small ? 'small' : ''}`}><span className="brand-mark" aria-hidden="true"><i /><i /><i /></span><span>DataSlate<span className="brand-period">.</span></span></span>;
}
export function Spinner({ label = 'Loading' }: { label?: string }) { return <span className="inline-loading" role="status"><LoaderCircle size={16} className="spin" />{label}</span>; }
export function ErrorNotice({ children, retry }: { children: ReactNode; retry?: () => void }) { return <div className="notice error" role="alert"><AlertCircle size={18} /><div>{children}{retry && <button className="text-button" onClick={retry}>Try again <ArrowRight size={14} /></button>}</div></div>; }
export function EmptyState({ icon, eyebrow, title, description, action }: { icon: ReactNode; eyebrow?: string; title: string; description: string; action?: ReactNode }) {
  return <div className="empty-state"><div className="empty-icon">{icon}</div>{eyebrow && <span className="eyebrow">{eyebrow}</span>}<h2>{title}</h2><p>{description}</p>{action}</div>;
}
export function Pill({ children, tone = 'neutral' }: { children: ReactNode; tone?: 'neutral' | 'green' | 'amber' }) { return <span className={`pill ${tone}`}>{tone === 'green' && <Check size={11} />}{children}</span>; }
export function Modal({ title, children, onClose }: { title: string; children: ReactNode; onClose: () => void }) {
  const dialog = useRef<HTMLElement>(null);
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    if (!dialog.current?.contains(document.activeElement)) dialog.current?.querySelector<HTMLElement>('button, input, select, [tabindex="0"]')?.focus();
    return () => previous?.focus();
  }, []);
  return <div className="modal-backdrop" onClick={onClose}><section ref={dialog} className="modal" role="dialog" aria-modal="true" aria-label={title} onClick={(e) => e.stopPropagation()} onKeyDown={(event) => {
    if (event.key === 'Escape') { event.stopPropagation(); onClose(); }
    if (event.key !== 'Tab') return;
    const elements = [...(dialog.current?.querySelectorAll<HTMLElement>('button:not([disabled]), input:not([disabled]), select:not([disabled]), a[href], [tabindex="0"]') ?? [])];
    const first = elements[0], last = elements[elements.length - 1];
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  }}><div className="modal-heading"><h2>{title}</h2><button className="icon-button" aria-label="Close dialog" onClick={onClose}><X size={18} /></button></div>{children}</section></div>;
}
export function download(filename: string, content: string, type = 'application/json') {
  const url = URL.createObjectURL(new Blob([content], { type }));
  const anchor = document.createElement('a'); anchor.href = url; anchor.download = filename; anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
