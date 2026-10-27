type LisaProactiveVoiceProps = {
  messages: string[];
};

export function LisaProactiveVoice({ messages }: LisaProactiveVoiceProps) {
  return <div className="tle-lisa-proactive-voice" aria-label="Lisa">
    <span className="tle-lisa-avatar" aria-hidden="true">L</span>
    <div>
      <b>Lisa</b>
      {messages.map((message, index) => <p key={index}>{message}</p>)}
    </div>
  </div>;
}
