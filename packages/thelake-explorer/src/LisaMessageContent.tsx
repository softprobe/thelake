import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

export function LisaMessageContent({ content }: { content: string }) {
  return <div className="tle-lisa-markdown"><ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown></div>;
}
