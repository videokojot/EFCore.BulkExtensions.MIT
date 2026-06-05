namespace EFCore.BulkExtensions;

using System;

public static class SpanSplitExtensions
{
    public static TokenSplitEnumerator<T> Split<T>(this ReadOnlySpan<T> span, ReadOnlySpan<T> delimiters)
        where T : IEquatable<T>
    {
        TokenSplitEnumerator<T> result = new TokenSplitEnumerator<T>(span, delimiters);
        return result;
    }

    public ref struct TokenSplitEnumerator<T> where T : IEquatable<T>
    {
        readonly ReadOnlySpan<T> _delimiters;
        ReadOnlySpan<T> _span;

        public TokenSplitEntry<T> Current { get; private set; }

        public TokenSplitEnumerator(ReadOnlySpan<T> span, ReadOnlySpan<T> delimiters)
        {
            _span = span;
            _delimiters = delimiters;
            Current = default;
        }

        public TokenSplitEnumerator<T> GetEnumerator()
        {
            TokenSplitEnumerator<T> result = this;
            return result;
        }

        public bool MoveNext()
        {
            ReadOnlySpan<T> span = _span;

            if (span.Length == 0)
            {
                return false;
            }

            int index = span.IndexOfAny(_delimiters);

            if (index == -1)
            {
                _span = ReadOnlySpan<T>.Empty;
                Current = new TokenSplitEntry<T>(span, ReadOnlySpan<T>.Empty);
                return true;
            }

            Current = new TokenSplitEntry<T>(span[..index], span.Slice(index, 1));
            _span = span[(index + 1)..];

            return true;
        }
    }

    public readonly ref struct TokenSplitEntry<T>
    {
        public ReadOnlySpan<T> Token { get; }
        public ReadOnlySpan<T> Delimiters { get; }
        
        public TokenSplitEntry(ReadOnlySpan<T> token, ReadOnlySpan<T> delimiters)
        {
            Token = token;
            Delimiters = delimiters;
        }

        public void Deconstruct(out ReadOnlySpan<T> token, out ReadOnlySpan<T> delimiters)
        {
            token = Token;
            delimiters = Delimiters;
        }

        public static implicit operator ReadOnlySpan<T>(TokenSplitEntry<T> entry)
        {
            ReadOnlySpan<T> result = entry.Token;
            return result;
        }
    }
}
