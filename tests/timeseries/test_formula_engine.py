# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Tests for the FormulaEngine and the Tokenizer."""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from frequenz.channels import Broadcast

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries.logical_meter._formula_engine import FormulaBuilder
from frequenz.sdk.timeseries.logical_meter._tokenizer import Token, Tokenizer, TokenType


class TestTokenizer:
    """Tests for the Tokenizer."""

    def test_1(self) -> None:
        """Test the tokenization of the formula: "#10 + #20 - (#5 * #4)"."""
        tokens = []
        lexer = Tokenizer("#10 + #20 - (#5 * #4)")
        for token in lexer:
            tokens.append(token)
        assert tokens == [
            Token(TokenType.COMPONENT_METRIC, "10"),
            Token(TokenType.OPER, "+"),
            Token(TokenType.COMPONENT_METRIC, "20"),
            Token(TokenType.OPER, "-"),
            Token(TokenType.OPER, "("),
            Token(TokenType.COMPONENT_METRIC, "5"),
            Token(TokenType.OPER, "*"),
            Token(TokenType.COMPONENT_METRIC, "4"),
            Token(TokenType.OPER, ")"),
        ]


class TestFormulaEngine:
    """Tests for the FormulaEngine."""

    def setup(self) -> None:
        """Initialize the channels required for a test.

        Because we can't create a __init__ or multiple instances of the Test class, we
        use the `setup` method as a constructor, and call it once before each test.
        """
        # pylint: disable=attribute-defined-outside-init
        self.comp_2 = Broadcast[Sample]("")
        self.comp_2_sender = self.comp_2.new_sender()

        self.comp_4 = Broadcast[Sample]("")
        self.comp_4_sender = self.comp_4.new_sender()

        self.comp_5 = Broadcast[Sample]("")
        self.comp_5_sender = self.comp_5.new_sender()
        # pylint: enable=attribute-defined-outside-init

    async def run_test(
        self,
        formula: str,
        postfix: str,
        io_pairs: List[Tuple[List[Optional[float]], Optional[float]]],
        nones_are_zeros: bool = False,
    ) -> None:
        """Run a formula test."""
        channels: Dict[str, Broadcast[Sample]] = {}
        builder = FormulaBuilder()
        for token in Tokenizer(formula):
            if token.type == TokenType.COMPONENT_METRIC:
                if token.value not in channels:
                    channels[token.value] = Broadcast(token.value)
                builder.push_metric(
                    f"#{token.value}",
                    channels[token.value].new_receiver(),
                    nones_are_zeros,
                )
            elif token.type == TokenType.OPER:
                builder.push_oper(token.value)
        engine = builder.build()

        assert repr(engine._steps) == postfix  # pylint: disable=protected-access

        now = datetime.now()
        tests_passed = 0
        for io_pair in io_pairs:
            io_input, io_output = io_pair
            assert all(
                await asyncio.gather(
                    *[
                        chan.new_sender().send(Sample(now, value))
                        for chan, value in zip(channels.values(), io_input)
                    ]
                )
            )
            assert (await engine.apply()).value == io_output
            tests_passed += 1
        assert tests_passed == len(io_pairs)

    async def test_simple(self) -> None:
        """Test simple formulas."""
        await self.run_test(
            "#2 - #4 + #5",
            "[#2, #4, -, #5, +]",
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([15.0, 17.0, 20.0], 18.0),
            ],
        )
        await self.run_test(
            "#2 + #4 - #5",
            "[#2, #4, #5, -, +]",
            [
                ([10.0, 12.0, 15.0], 7.0),
                ([15.0, 17.0, 20.0], 12.0),
            ],
        )
        await self.run_test(
            "#2 * #4 + #5",
            "[#2, #4, *, #5, +]",
            [
                ([10.0, 12.0, 15.0], 135.0),
                ([15.0, 17.0, 20.0], 275.0),
            ],
        )
        await self.run_test(
            "#2 * #4 / #5",
            "[#2, #4, #5, /, *]",
            [
                ([10.0, 12.0, 15.0], 8.0),
                ([15.0, 17.0, 20.0], 12.75),
            ],
        )
        await self.run_test(
            "#2 / #4 - #5",
            "[#2, #4, /, #5, -]",
            [
                ([6.0, 12.0, 15.0], -14.5),
                ([15.0, 20.0, 20.0], -19.25),
            ],
        )

    async def test_compound(self) -> None:
        """Test compound formulas."""
        await self.run_test(
            "#2 + #4 - #5 * #6",
            "[#2, #4, #5, #6, *, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([15.0, 17.0, 20.0, 1.5], 2.0),
            ],
        )
        await self.run_test(
            "#2 + (#4 - #5) * #6",
            "[#2, #4, #5, -, #6, *, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], 4.0),
                ([15.0, 17.0, 20.0, 1.5], 10.5),
            ],
        )
        await self.run_test(
            "(#2 + #4 - #5) * #6",
            "[#2, #4, #5, -, +, #6, *]",
            [
                ([10.0, 12.0, 15.0, 2.0], 14.0),
                ([15.0, 17.0, 20.0, 1.5], 18.0),
            ],
        )
        await self.run_test(
            "(#2 + #4 - #5) / #6",
            "[#2, #4, #5, -, +, #6, /]",
            [
                ([10.0, 12.0, 15.0, 2.0], 3.5),
                ([15.0, 17.0, 20.0, 1.5], 8.0),
            ],
        )
        await self.run_test(
            "#2 + #4 - (#5 / #6)",
            "[#2, #4, #5, #6, /, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], 14.5),
                ([15.0, 17.0, 20.0, 5.0], 28.0),
            ],
        )

    async def test_nones_are_zeros(self) -> None:
        """Test that `None`s are treated as zeros when configured."""
        await self.run_test(
            "#2 - #4 + #5",
            "[#2, #4, -, #5, +]",
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([None, 12.0, 15.0], 3.0),
                ([10.0, None, 15.0], 25.0),
                ([15.0, 17.0, 20.0], 18.0),
                ([15.0, None, None], 15.0),
            ],
            True,
        )

        await self.run_test(
            "#2 + #4 - (#5 * #6)",
            "[#2, #4, #5, #6, *, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([10.0, 12.0, 15.0, None], 22.0),
                ([10.0, None, 15.0, 2.0], -20.0),
                ([15.0, 17.0, 20.0, 5.0], -68.0),
                ([15.0, 17.0, None, 5.0], 32.0),
            ],
            True,
        )

    async def test_nones_are_not_zeros(self) -> None:
        """Test that calculated values are `None` on input `None`s."""
        await self.run_test(
            "#2 - #4 + #5",
            "[#2, #4, -, #5, +]",
            [
                ([10.0, 12.0, 15.0], 13.0),
                ([None, 12.0, 15.0], None),
                ([10.0, None, 15.0], None),
                ([15.0, 17.0, 20.0], 18.0),
                ([15.0, None, None], None),
            ],
            False,
        )

        await self.run_test(
            "#2 + #4 - (#5 * #6)",
            "[#2, #4, #5, #6, *, -, +]",
            [
                ([10.0, 12.0, 15.0, 2.0], -8.0),
                ([10.0, 12.0, 15.0, None], None),
                ([10.0, None, 15.0, 2.0], None),
                ([15.0, 17.0, 20.0, 5.0], -68.0),
                ([15.0, 17.0, None, 5.0], None),
            ],
            False,
        )
