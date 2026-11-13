INSERT INTO traces VALUES (
    '{{session_id}}', '{{span_id}}', {{ts}}, {{end}}, '{{status}}', '{{message_type}}',
    {{agent}}, '{{observation_type}}', {{in_tok}}, {{out_tok}}, {{tot_tok}}, {{cost}},
    {{user_id}}, {{model}}, DATE '{{day}}', '{{trace_id}}', {{parent_span_id}}
);
